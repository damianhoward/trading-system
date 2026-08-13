package com.damianhoward.tradingsystem.health

import com.damianhoward.tradingsystem.consume.ConsumerHealth
import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.position.LedgerSnapshot
import com.damianhoward.tradingsystem.position.Reconciliation
import com.damianhoward.tradingsystem.position.SymbolTotals
import com.damianhoward.tradingsystem.position.View
import com.damianhoward.tradingsystem.position.ViewTotals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class ReadinessTest {
    private class SteppingClock(
        private var now: Instant = Instant.parse("2026-07-19T10:00:00Z"),
    ) : Clock() {
        override fun instant(): Instant = now

        override fun getZone(): ZoneOffset = ZoneOffset.UTC

        override fun withZone(zone: java.time.ZoneId): Clock = this

        fun advance(duration: Duration) {
            now = now.plus(duration)
        }
    }

    private val clock = SteppingClock()
    private val consumer = ConsumerHealth("fills", clock)
    private var positionsOffset: Long? = 9L
    private var limitsOffset: Long? = 9L
    private var reconciliation: Reconciliation? = agreeingReconciliation()

    private fun agreeingReconciliation() = reconciliationOf(listOf(SymbolTotals("SIM", 4, 4)))

    /**
     * A pass over the fixture's ledger. Unless a case says otherwise the two in-memory views
     * mirror the ledger at its own offset, so they are judged and agree — which leaves each case
     * free to perturb exactly one thing and keeps the assertion about that thing.
     */
    private fun reconciliationOf(
        totals: List<SymbolTotals>,
        ledgerOffset: Long? = 9L,
        views: Map<View, ViewTotals> = mirroring(totals, ledgerOffset),
    ) = Reconciliation.of(LedgerSnapshot(ledgerOffset, totals), views, clock.millis())

    private fun mirroring(
        totals: List<SymbolTotals>,
        offset: Long?,
    ): Map<View, ViewTotals> {
        val quantities = totals.associate { it.symbol to it.ledgerQuantity }
        return mapOf(
            View.POSITION_BOOK to ViewTotals(offset, quantities),
            View.LIMITS to ViewTotals(offset, quantities),
        )
    }

    private fun readiness(databaseOk: Boolean = true) =
        Readiness(
            consumers = listOf(consumer),
            databaseOk = { databaseOk },
            deadLettersPublished = { 3 },
            deadLettersFailed = { 1 },
            positionsView = { positionsOffset?.let { ConsumerProgress(it, 1000) } },
            limitsView = { limitsOffset?.let { ConsumerProgress(it, 1000) } },
            reconciliation = { reconciliation },
            clock = clock,
        )

    private fun healthyConsumer() {
        consumer.started()
        consumer.assigned(1)
        consumer.polled()
    }

    @Test
    fun `ready only when every consumer is alive, assigned, and polling, and the database answers`() {
        healthyConsumer()
        val probe = readiness().probe()
        assertTrue(probe.ready)
        assertTrue(probe.json.contains(""""fills":{"ok":true"""), probe.json)
        assertTrue(probe.json.contains(""""deadLetters":{"published":3,"failed":1}"""), probe.json)
    }

    @Test
    fun `metrics render the same snapshot readiness evaluates`() {
        healthyConsumer()
        val metrics = readiness().metrics()
        assertTrue(metrics.contains("trading_system_ready 1"), metrics)
        assertTrue(metrics.contains("trading_system_database_up 1"), metrics)
        assertTrue(metrics.contains("""trading_system_consumer_up{consumer="fills"} 1"""), metrics)
        assertTrue(metrics.contains("trading_system_view_offset{view=\"positions\"} 9"), metrics)
        assertTrue(metrics.contains("trading_system_views_coherent 1"), metrics)
        assertTrue(metrics.contains("""trading_system_ledger_agrees{view="positions"} 1"""), metrics)
        assertTrue(metrics.contains("""trading_system_ledger_agrees{view="limits"} 1"""), metrics)
        assertTrue(metrics.contains("""trading_system_ledger_divergent_symbols{view="position_book"} 0"""), metrics)
        assertTrue(metrics.contains("trading_system_dead_letters_published_total 3"), metrics)
        assertTrue(metrics.contains("trading_system_dead_letters_failed_total 1"), metrics)
        // Every series is declared: an unTYPEd sample is accepted but loses its counter/gauge
        // semantics, and rate() over a gauge is a silently wrong answer rather than an error.
        for (line in metrics.lines().filter { it.isNotBlank() && !it.startsWith("#") }) {
            val name = line.substringBefore('{').substringBefore(' ')
            assertTrue(metrics.contains("# TYPE $name "), "$name has no TYPE line")
            assertTrue(metrics.contains("# HELP $name "), "$name has no HELP line")
        }
    }

    @Test
    fun `metrics count divergences rather than labelling by symbol`() {
        healthyConsumer()
        reconciliation = reconciliationOf(listOf(SymbolTotals("SIM", 4, 7), SymbolTotals("AAPL", 2, 2)))
        val metrics = readiness().metrics()
        assertTrue(metrics.contains("""trading_system_ledger_agrees{view="positions"} 0"""), metrics)
        assertTrue(metrics.contains("""trading_system_ledger_divergent_symbols{view="positions"} 1"""), metrics)
        // One series per symbol that ever diverged would be kept forever by the collector.
        assertFalse(metrics.contains("SIM"), "a symbol must not become a label: $metrics")
    }

    @Test
    fun `metrics carry no free text, because the endpoint is unauthenticated`() {
        healthyConsumer()
        consumer.failed(java.sql.SQLRecoverableException("(HOST=db.internal)(SERVICE_NAME=trading_tp)", "08006", 17002))
        val metrics = readiness().metrics()
        assertFalse(metrics.contains("HOST="), metrics)
        assertFalse(metrics.contains("SQLRecoverableException"), metrics)
        assertTrue(metrics.contains("""trading_system_consumer_up{consumer="fills"} 0"""), metrics)
    }

    @Test
    fun `a consumer that has never polled is not ready`() {
        consumer.started()
        consumer.assigned(1)
        val probe = readiness().probe()
        assertFalse(probe.ready)
        assertTrue(probe.json.contains(""""pollAgeMillis":null"""), probe.json)
    }

    @Test
    fun `a consumer that stopped polling goes not-ready once the poll age passes the ceiling`() {
        healthyConsumer()
        clock.advance(Duration.ofSeconds(29))
        assertTrue(readiness().probe().ready, "29s is within the 30s ceiling")

        clock.advance(Duration.ofSeconds(2))
        val probe = readiness().probe()
        assertFalse(probe.ready, "a silent consumer is a broken consumer, whatever the thread state says")
        assertEquals(31_000L, consumer.pollAgeMillis())
    }

    @Test
    fun `a dead consumer thread reports the whole cause chain by type and vendor code`() {
        healthyConsumer()
        consumer.failed(IllegalStateException("retries exhausted", java.sql.SQLException("unable to extend", "53100", 1653)))
        val probe = readiness().probe()
        assertFalse(probe.ready)
        assertTrue(probe.json.contains("java.lang.IllegalStateException"), probe.json)
        assertTrue(probe.json.contains("java.sql.SQLException[1653]"), "the wrapper without the root cause tells an operator nothing")
        assertTrue(probe.json.contains(""""threadAlive":false"""), probe.json)
    }

    @Test
    fun `the fatal field carries no exception message, because readyz is public`() {
        healthyConsumer()
        val descriptor = "IO Error: (HOST=adb.uk-london-1.oraclecloud.com)(SERVICE_NAME=trading_tp)(USER=TRADING)"
        consumer.failed(java.sql.SQLRecoverableException(descriptor, "08006", 17002))
        val probe = readiness().probe()
        assertFalse(probe.ready)
        assertTrue(probe.json.contains("java.sql.SQLRecoverableException[17002]"), probe.json)
        assertFalse(probe.json.contains("HOST="), "an unauthenticated probe must not publish a connection descriptor")
        assertFalse(probe.json.contains("SERVICE_NAME"), probe.json)
        assertFalse(probe.json.contains("TRADING"), probe.json)
    }

    @Test
    fun `a dead database fails readiness even with healthy consumers`() {
        healthyConsumer()
        val probe = readiness(databaseOk = false).probe()
        assertFalse(probe.ready)
        assertTrue(probe.json.contains(""""database":{"ok":false}"""), probe.json)
    }

    @Test
    fun `a cleanly stopped consumer is not ready`() {
        healthyConsumer()
        consumer.stopped()
        assertFalse(readiness().probe().ready)
    }

    @Test
    fun `matching views are coherent and two empty views count as matching`() {
        healthyConsumer()
        assertTrue(readiness().probe().json.contains(""""coherent":true"""))

        positionsOffset = null
        limitsOffset = null
        val probe = readiness().probe()
        assertTrue(probe.ready, "a fresh install has consumed nothing on either path")
        assertTrue(probe.json.contains(""""positionsOffset":null,"limitsOffset":null,"coherent":true"""), probe.json)
    }

    @Test
    fun `diverged views stay ready within the grace window and fail it once stuck`() {
        healthyConsumer()
        positionsOffset = 7
        val probes = readiness()

        val within = probes.probe()
        assertTrue(within.ready, "independent consumers legitimately sit apart mid-burst")
        assertTrue(within.json.contains(""""coherent":false"""), within.json)
        assertTrue(within.json.contains(""""incoherentForMillis":0"""), within.json)

        clock.advance(Duration.ofSeconds(31))
        consumer.polled()
        val stuck = probes.probe()
        assertFalse(stuck.ready, "views apart past the grace window mean a projection is stuck")
        assertTrue(stuck.json.contains(""""positionsOffset":7,"limitsOffset":9"""), stuck.json)
        assertTrue(stuck.json.contains(""""incoherentForMillis":31000"""), stuck.json)
    }

    @Test
    fun `views that converge again reset the incoherence clock`() {
        healthyConsumer()
        positionsOffset = 7
        val probes = readiness()
        probes.probe()
        clock.advance(Duration.ofSeconds(31))
        consumer.polled()
        assertFalse(probes.probe().ready)

        positionsOffset = 9
        assertTrue(probes.probe().ready, "caught-up views are coherent again")

        positionsOffset = 8
        clock.advance(Duration.ofSeconds(29))
        consumer.polled()
        assertTrue(probes.probe().ready, "a fresh divergence starts a fresh grace window")
    }

    @Test
    fun `one empty view beside one consumed view is incoherent`() {
        healthyConsumer()
        positionsOffset = null
        val probes = readiness()
        probes.probe()
        clock.advance(Duration.ofSeconds(31))
        consumer.polled()
        val probe = probes.probe()
        assertFalse(probe.ready, "one view at the ledger and one at nothing cannot both be right")
        assertTrue(probe.json.contains(""""positionsOffset":null,"limitsOffset":9,"coherent":false"""), probe.json)
    }

    @Test
    fun `a book that reconciles reports the check and stays ready`() {
        healthyConsumer()
        val probe = readiness().probe()
        assertTrue(probe.ready)
        assertTrue(probe.json.contains(""""ledger":{"ok":true,"checkedAgeMillis":0"""), probe.json)
        assertTrue(probe.json.contains(""""positions":{"ok":true,"symbolsChecked":1,"divergences":[]"""), probe.json)
    }

    @Test
    fun `a position that has drifted from its ledger fails the probe and names the gap`() {
        healthyConsumer()
        reconciliation = reconciliationOf(listOf(SymbolTotals("SIM", ledgerQuantity = 10, positionQuantity = 13)))

        val probe = readiness().probe()

        assertFalse(probe.ready, "a book that disagrees with its fills is not safe to serve")
        assertTrue(
            probe.json.contains(
                """{"symbol":"SIM","ledgerQuantity":10,"viewQuantity":13,"difference":3}""",
            ),
            probe.json,
        )
    }

    @Test
    fun `coherent offsets do not rescue a divergent book`() {
        // The distinction the whole check exists for: both views have read the same amount of
        // stream, and the number one of them holds is still wrong.
        healthyConsumer()
        reconciliation = reconciliationOf(listOf(SymbolTotals("SIM", 10, 13)))

        val probe = readiness().probe()

        assertTrue(probe.json.contains(""""coherent":true"""), probe.json)
        assertFalse(probe.ready)
    }

    @Test
    fun `a limits view that disagrees with the ledger at a matching offset fails the probe`() {
        // Nothing else in the service would notice. The limits consumer never reads the position
        // book by design, so its exposures can be wrong while positions, risk and the dashboard
        // all agree with each other.
        healthyConsumer()
        val totals = listOf(SymbolTotals("SIM", 10, 10))
        reconciliation =
            reconciliationOf(
                totals,
                views =
                    mirroring(totals, 9L) +
                        mapOf(View.LIMITS to ViewTotals(9L, mapOf("SIM" to 4L))),
            )

        val probe = readiness().probe()

        assertFalse(probe.ready, "an independent view holding the wrong exposure is not safe to serve")
        assertTrue(probe.json.contains(""""limits":{"ok":false"""), probe.json)
        assertTrue(
            probe.json.contains("""{"symbol":"SIM","ledgerQuantity":10,"viewQuantity":4,"difference":-6}"""),
            probe.json,
        )
        assertTrue(probe.json.contains(""""positions":{"ok":true"""), "only the guilty view fails: ${probe.json}")
    }

    @Test
    fun `a position book that has drifted fails the probe, because the risk report is priced from it`() {
        healthyConsumer()
        val totals = listOf(SymbolTotals("SIM", 10, 10))
        reconciliation =
            reconciliationOf(
                totals,
                views =
                    mirroring(totals, 9L) +
                        mapOf(View.POSITION_BOOK to ViewTotals(9L, mapOf("SIM" to 25L))),
            )

        val probe = readiness().probe()

        assertFalse(probe.ready)
        assertTrue(probe.json.contains(""""position_book":{"ok":false"""), probe.json)
    }

    @Test
    fun `a view behind the ledger is inconclusive, not divergent, and does not fail the probe`() {
        // The race the offset guard exists for. Judging a view mid-flight would report a
        // divergence for every fill in transit, and a check that cries wolf gets turned off.
        healthyConsumer()
        val totals = listOf(SymbolTotals("SIM", 10, 10))
        reconciliation =
            reconciliationOf(
                totals,
                views =
                    mirroring(totals, 9L) +
                        mapOf(View.LIMITS to ViewTotals(7L, mapOf("SIM" to 4L))),
            )

        val probe = readiness().probe()

        assertTrue(probe.ready, "a view that has not caught up has not been shown to be wrong")
        assertTrue(probe.json.contains(""""inconclusive":{"viewOffset":7,"ledgerOffset":9"""), probe.json)
        assertTrue(probe.json.contains(""""divergences":[]"""), "no verdict, so no divergence: ${probe.json}")
    }

    @Test
    fun `a view that stays unjudgeable eventually fails, because a check asserting nothing is not a pass`() {
        healthyConsumer()
        val totals = listOf(SymbolTotals("SIM", 10, 10))

        fun behind() =
            reconciliationOf(
                totals,
                views = mirroring(totals, 9L) + mapOf(View.LIMITS to ViewTotals(7L, mapOf("SIM" to 10L))),
            )
        reconciliation = behind()
        assertTrue(readiness().probe().ready)

        val probe = readiness()
        assertTrue(probe.probe().ready, "the clock starts on the first unjudgeable pass")
        clock.advance(Duration.ofMinutes(2))
        consumer.polled()
        reconciliation = behind()
        assertTrue(probe.probe().ready, "2m is within the three-pass budget")

        clock.advance(Duration.ofMinutes(2))
        consumer.polled()
        reconciliation = behind()
        assertFalse(probe.probe().ready, "a view that can never be judged is not a view that agrees")
    }

    @Test
    fun `an inconclusive view publishes no verdict rather than a zero`() {
        healthyConsumer()
        val totals = listOf(SymbolTotals("SIM", 10, 10))
        reconciliation =
            reconciliationOf(
                totals,
                views = mirroring(totals, 9L) + mapOf(View.LIMITS to ViewTotals(7L, mapOf("SIM" to 10L))),
            )

        val metrics = readiness().metrics()

        // Pinned at 0 it reads as a standing divergence; pinned at 1, as an assertion nothing made.
        assertFalse(metrics.contains("""trading_system_ledger_agrees{view="limits"}"""), metrics)
        assertTrue(metrics.contains("""trading_system_ledger_check_inconclusive{view="limits"} 1"""), metrics)
        assertTrue(metrics.contains("""trading_system_ledger_check_inconclusive{view="positions"} 0"""), metrics)
        assertTrue(metrics.contains("""trading_system_ledger_agrees{view="positions"} 1"""), metrics)
    }

    @Test
    fun `a reconciliation that has stopped being refreshed fails once it goes stale`() {
        healthyConsumer()
        clock.advance(Duration.ofMinutes(4))
        consumer.polled()
        assertTrue(readiness().probe().ready, "4m is within the 5m staleness budget")

        clock.advance(Duration.ofMinutes(2))
        consumer.polled()
        val probe = readiness().probe()
        assertFalse(probe.ready, "a check that stopped running is not a book that reconciles")
        assertTrue(probe.json.contains(""""checkedAgeMillis":360000"""), probe.json)
    }

    @Test
    fun `a probe taken before the first reconciliation is not ready`() {
        healthyConsumer()
        reconciliation = null

        val probe = readiness().probe()

        assertFalse(probe.ready, "nothing has established the book agrees with its ledger yet")
        assertTrue(probe.json.contains(""""ledger":{"ok":false,"checkedAgeMillis":null,"views":{}}"""), probe.json)
    }
}
