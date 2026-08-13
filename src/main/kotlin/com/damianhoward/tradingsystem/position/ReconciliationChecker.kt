package com.damianhoward.tradingsystem.position

import java.time.Clock
import java.time.Duration

/**
 * Runs the conservation check on a schedule and holds the most recent result for `/readyz`.
 *
 * The check is not evaluated on the probe itself. `/readyz` is public and unauthenticated, and
 * the check aggregates a table that only grows, so computing it per request would let anyone
 * decide how much database work this service does. Running it on a timer and serving the cached
 * answer keeps that cost fixed and known.
 *
 * The result therefore has an age, and the age is part of the signal rather than metadata: a
 * checker that has stopped running leaves a passing result behind it, which reads exactly like a
 * book that reconciles. [com.damianhoward.tradingsystem.health.Readiness] fails on staleness for
 * the same reason it fails on a consumer's poll age — "it last succeeded" is not the same claim
 * as "it is succeeding".
 *
 * One ledger read serves every view, so the three verdicts in a pass are all taken against the
 * same instant of the ledger. Reading it per view would let them disagree about what the ledger
 * held, which is the same defect the single-statement read exists to prevent, one level up.
 *
 * [inMemoryViews] is read after the ledger, not before. A view that moves in between reports an
 * offset ahead of the snapshot and is judged inconclusive rather than divergent — the ordering
 * makes the race show up as "cannot say", which is recoverable, instead of as a false divergence,
 * which is not.
 */
class ReconciliationChecker(
    private val ledgerSnapshot: () -> LedgerSnapshot,
    private val inMemoryViews: Map<View, () -> ViewTotals> = emptyMap(),
    private val clock: Clock = Clock.systemUTC(),
) {
    @Volatile
    private var latest: Reconciliation? = null

    fun latest(): Reconciliation? = latest

    /** Age of the last successful pass, or null before the first one completes. */
    fun ageMillis(): Long? = latest?.let { clock.millis() - it.checkedAtMillis }

    /**
     * Runs one pass and publishes its result. Throws nothing: this is handed to a
     * ScheduledExecutorService, which abandons a repeating task permanently the first time it
     * throws and reports that nowhere, so an escaping exception would stop reconciliation for the
     * life of the process while the last good result stood as if it were current.
     *
     * A failed pass is not swallowed, it is expressed as absence of a fresh one: the previous
     * result stays put and ages, and readiness fails on that age. The cause is printed because it
     * is the only place it exists — the age tells an operator the check is failing, not why.
     */
    fun run() {
        try {
            val ledger = ledgerSnapshot()
            val views = inMemoryViews.mapValues { (_, totals) -> totals() }
            latest = Reconciliation.of(ledger, views, clock.millis())
        } catch (e: Exception) {
            System.err.println("view reconciliation failed:")
            e.printStackTrace()
        }
    }

    companion object {
        /**
         * Every minute. The condition is corruption rather than a fast-moving quantity, so this is
         * not about resolution — it is that the external probe runs every five minutes, and a
         * one-minute pass guarantees the answer a probe reads is at most one interval behind the
         * database rather than nearly a whole probe cycle.
         */
        val INTERVAL: Duration = Duration.ofMinutes(1)

        /**
         * Five missed passes. A real database outage already fails readiness through the
         * connectivity check, so this budget exists for the narrower case where the database is
         * fine and the checker is not — a dead scheduler, or a pass that throws every time. Wide
         * enough that a single slow query does not flap the probe during a deploy.
         */
        val MAX_AGE: Duration = Duration.ofMinutes(5)
    }
}
