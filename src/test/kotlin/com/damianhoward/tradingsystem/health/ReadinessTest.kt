package com.damianhoward.tradingsystem.health

import com.damianhoward.tradingsystem.consume.ConsumerHealth
import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.position.Reconciliation
import com.damianhoward.tradingsystem.position.SymbolTotals
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

    private fun agreeingReconciliation() = Reconciliation.of(listOf(SymbolTotals("SIM", 4, 4)), checkedAtMillis = clock.millis())

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
        assertTrue(probe.json.contains(""""symbolsChecked":1,"divergences":[]"""), probe.json)
    }

    @Test
    fun `a position that has drifted from its ledger fails the probe and names the gap`() {
        healthyConsumer()
        reconciliation =
            Reconciliation.of(
                listOf(SymbolTotals("SIM", ledgerQuantity = 10, positionQuantity = 13)),
                checkedAtMillis = clock.millis(),
            )

        val probe = readiness().probe()

        assertFalse(probe.ready, "a book that disagrees with its fills is not safe to serve")
        assertTrue(
            probe.json.contains(
                """{"symbol":"SIM","ledgerQuantity":10,"positionQuantity":13,"difference":3}""",
            ),
            probe.json,
        )
    }

    @Test
    fun `coherent offsets do not rescue a divergent book`() {
        // The distinction the whole check exists for: both views have read the same amount of
        // stream, and the number one of them holds is still wrong.
        healthyConsumer()
        reconciliation =
            Reconciliation.of(listOf(SymbolTotals("SIM", 10, 13)), checkedAtMillis = clock.millis())

        val probe = readiness().probe()

        assertTrue(probe.json.contains(""""coherent":true"""), probe.json)
        assertFalse(probe.ready)
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
        assertTrue(probe.json.contains(""""ledger":{"ok":false,"checkedAgeMillis":null"""), probe.json)
        assertTrue(probe.json.contains(""""symbolsChecked":null"""), probe.json)
    }
}
