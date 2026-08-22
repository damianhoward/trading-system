package com.damianhoward.tradingsystem.position

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ReconciliationCheckerTest {
    private class SteppingClock(
        private var now: Instant = Instant.parse("2026-08-10T09:00:00Z"),
    ) : Clock() {
        override fun instant(): Instant = now

        override fun getZone(): ZoneOffset = ZoneOffset.UTC

        override fun withZone(zone: ZoneId): Clock = this

        fun advance(duration: Duration) {
            now = now.plus(duration)
        }
    }

    private val clock = SteppingClock()
    private val agreeing = LedgerSnapshot(4, listOf(SymbolTotals("SIM", 4, 4)))

    @Test
    fun `nothing is published before the first pass runs`() {
        val checker = ReconciliationChecker({ LedgerSnapshot(null, emptyList()) }, clock = clock)

        assertNull(checker.latest())
        assertNull(checker.ageMillis())
    }

    @Test
    fun `a pass publishes the book's verdict stamped with the time it was taken`() {
        val checker =
            ReconciliationChecker({ agreeing }, clock = clock)

        checker.run()

        val latest = checker.latest()!!
        assertTrue(latest.agrees)
        assertEquals(clock.millis(), latest.checkedAtMillis)
        assertEquals(0, checker.ageMillis())
    }

    @Test
    fun `the published result ages, which is what makes a stopped checker visible`() {
        val checker = ReconciliationChecker({ agreeing }, clock = clock)
        checker.run()

        clock.advance(Duration.ofMinutes(7))

        assertEquals(Duration.ofMinutes(7).toMillis(), checker.ageMillis())
    }

    @Test
    fun `a failing pass leaves the previous result standing and lets it age rather than throwing`() {
        var fail = false
        val checker =
            ReconciliationChecker({
                if (fail) throw IllegalStateException("database unreachable")
                agreeing
            }, clock = clock)
        checker.run()
        val firstPass = checker.latest()!!.checkedAtMillis

        fail = true
        clock.advance(Duration.ofMinutes(3))
        checker.run()

        assertEquals(firstPass, checker.latest()!!.checkedAtMillis)
        assertEquals(Duration.ofMinutes(3).toMillis(), checker.ageMillis())
    }

    @Test
    fun `a divergence is published as one`() {
        val checker =
            ReconciliationChecker(
                { LedgerSnapshot(4, listOf(SymbolTotals("SIM", ledgerQuantity = 4, positionQuantity = 9))) },
                clock = clock,
            )

        checker.run()

        assertFalse(checker.latest()!!.agrees)
        assertEquals(
            5,
            checker
                .latest()!!
                .verdict(View.POSITIONS)!!
                .divergences
                .single()
                .difference,
        )
    }

    @Test
    fun `one ledger read serves every view, so no two verdicts disagree about what the ledger held`() {
        // Reading it per view would let the three pass results describe different instants, which
        // is the defect the single-statement read prevents one level down.
        val reads = AtomicInteger()
        val checker =
            ReconciliationChecker(
                ledgerSnapshot = {
                    reads.incrementAndGet()
                    agreeing
                },
                inMemoryViews =
                    mapOf(
                        View.POSITION_BOOK to { ViewTotals(4, mapOf("SIM" to 4L)) },
                        View.EXPOSURE to { ViewTotals(4, mapOf("SIM" to 4L)) },
                    ),
                clock = clock,
            )

        checker.run()

        assertEquals(1, reads.get())
        assertEquals(3, checker.latest()!!.verdicts.size)
        assertTrue(checker.latest()!!.agrees)
    }

    @Test
    fun `an in-memory view is read after the ledger, so a view that moves reads as unjudged`() {
        // Ordering is the whole guard. Read first, a view could be compared against a ledger that
        // has since grown past it and be reported divergent for fills that simply had not landed
        // when it was sampled.
        var ledgerOffset = 4L
        val checker =
            ReconciliationChecker(
                ledgerSnapshot = { LedgerSnapshot(ledgerOffset, listOf(SymbolTotals("SIM", 4, 4))) },
                inMemoryViews = mapOf(View.EXPOSURE to { ViewTotals(ledgerOffset + 1, mapOf("SIM" to 4L)) }),
                clock = clock,
            )

        checker.run()

        val limits = checker.latest()!!.verdict(View.EXPOSURE)!!
        assertEquals(Inconclusive(viewOffset = 5, ledgerOffset = 4), limits.inconclusive)
        assertTrue(limits.divergences.isEmpty())
    }

    @Test
    fun `the default clock is the system one, which is what production runs on`() {
        val before = System.currentTimeMillis()
        val checker = ReconciliationChecker(ledgerSnapshot = { LedgerSnapshot(1, listOf(SymbolTotals("SIM", 1, 1))) })

        checker.run()

        val checkedAt = checker.latest()!!.checkedAtMillis
        assertTrue(checkedAt in before..System.currentTimeMillis(), "stamped from the wall clock, not a fixture")
    }

    @Test
    fun `the schedule is faster than the probe it feeds, and the staleness budget is wider still`() {
        // The two constants are only meaningful relative to each other and to the five-minute
        // external probe; asserting the ordering keeps a later tweak from inverting them.
        assertTrue(ReconciliationChecker.INTERVAL < ReconciliationChecker.MAX_AGE)
        assertEquals(Duration.ofMinutes(1), ReconciliationChecker.INTERVAL)
        assertEquals(Duration.ofMinutes(5), ReconciliationChecker.MAX_AGE)
    }

    @Test
    fun `a throwing pass does not cancel the schedule that drives it`() {
        // The reason run() catches at all. scheduleWithFixedDelay abandons a repeating task for the
        // life of the process the first time it throws, and reports that nowhere — so this asserts
        // the property Main depends on, against the real executor rather than a description of it.
        val attempts = AtomicInteger()
        val checker =
            ReconciliationChecker({
                attempts.incrementAndGet()
                throw IllegalStateException("database unreachable")
            }, clock = clock)
        val executor = Executors.newSingleThreadScheduledExecutor()

        try {
            executor.scheduleWithFixedDelay(checker::run, 0, 5, TimeUnit.MILLISECONDS)
            val ranRepeatedly =
                generateSequence { attempts.get() }
                    .take(200)
                    .onEach { Thread.sleep(5) }
                    .any { it >= 3 }
            assertTrue(ranRepeatedly, "the schedule stopped after the first failure")
        } finally {
            executor.shutdownNow()
        }
    }
}
