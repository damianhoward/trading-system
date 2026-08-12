package com.damianhoward.tradingsystem.consume

import java.sql.SQLException
import java.time.Clock

/**
 * One consumer's operational truth, written by its poll thread and read by the readiness probe.
 * "The HTTP server answers" says nothing about ingestion; this does: the thread is alive, it
 * holds an assignment, it polled recently, and it has not died on an unexpected exception.
 */
class ConsumerHealth(
    val name: String,
    private val clock: Clock = Clock.systemUTC(),
) {
    @Volatile
    var threadAlive: Boolean = false
        private set

    @Volatile
    var assigned: Boolean = false
        private set

    @Volatile
    var lastPollMillis: Long = 0
        private set

    @Volatile
    var fatal: String? = null
        private set

    fun started() {
        threadAlive = true
    }

    fun assigned(partitions: Int) {
        assigned = partitions > 0
    }

    fun polled() {
        lastPollMillis = clock.millis()
    }

    fun stopped() {
        threadAlive = false
    }

    fun failed(error: Throwable) {
        // The whole cause chain, because "retries exhausted" without what failed underneath tells
        // an operator nothing — the probe must name the root cause, not the wrapper.
        //
        // Types and vendor codes only. This field is served by /readyz, which is public and
        // unauthenticated, and an exception message is free text this process does not author: a
        // JDBC failure carries the connection descriptor — host, port, service name — in its
        // message, and a parse failure carries the input. The type chain plus the SQL error code
        // still separates "database unreachable" from "constraint violated" from "retries
        // exhausted", which is what the probe is read for. Whoever needs the message has the
        // journal: every caller of this hands the same error to onFatal, which prints it.
        fatal =
            generateSequence(error) { it.cause }
                .joinToString(" <- ") { it.javaClass.name + vendorCode(it) }
        threadAlive = false
    }

    private fun vendorCode(error: Throwable): String = if (error is SQLException && error.errorCode != 0) "[${error.errorCode}]" else ""

    /** Milliseconds since the last completed poll, or null before the first one. */
    fun pollAgeMillis(): Long? = if (lastPollMillis == 0L) null else clock.millis() - lastPollMillis
}
