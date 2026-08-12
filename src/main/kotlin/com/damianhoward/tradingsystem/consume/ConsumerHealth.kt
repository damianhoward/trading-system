package com.damianhoward.tradingsystem.consume

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
        // The whole cause chain: "retries exhausted" without the ORA code underneath tells an
        // operator nothing — the probe must name the root cause, not the wrapper.
        fatal =
            generateSequence(error) { it.cause }
                .joinToString(" <- ") { "${it.javaClass.name}: ${it.message}" }
        threadAlive = false
    }

    /** Milliseconds since the last completed poll, or null before the first one. */
    fun pollAgeMillis(): Long? = if (lastPollMillis == 0L) null else clock.millis() - lastPollMillis
}
