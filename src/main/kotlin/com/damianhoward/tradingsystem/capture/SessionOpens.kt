package com.damianhoward.tradingsystem.capture

import com.damianhoward.tradingsystem.consume.Fill
import java.math.BigDecimal

/**
 * Per-symbol session-open marks, derived from fill event time: a symbol's open is the price of
 * its first fill on its latest trading day (UTC, by the fill's own timestamp — no wall clock).
 * Derived purely from the fills that built the book, so a restart warmed from the ledger
 * reproduces the same opens and day PnL no longer resets when the process does. A fill on a
 * later UTC day rolls that symbol's open to its price.
 *
 * Thread-safe: the consumer thread observes, web threads read.
 */
class SessionOpens {
    private data class Open(
        val epochDay: Long,
        val price: BigDecimal,
    )

    private val lock = Any()
    private val opens = HashMap<String, Open>()

    fun observe(fill: Fill) {
        val day = Math.floorDiv(fill.timeMillis, MILLIS_PER_DAY)
        synchronized(lock) {
            val current = opens[fill.symbol]
            if (current == null || day > current.epochDay) {
                opens[fill.symbol] = Open(day, fill.price)
            }
        }
    }

    /** The symbol's open for its latest traded day, or null before its first fill. */
    fun openFor(symbol: String): BigDecimal? = synchronized(lock) { opens[symbol]?.price }

    companion object {
        private const val MILLIS_PER_DAY = 86_400_000L
    }
}
