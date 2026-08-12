package com.damianhoward.tradingsystem.position

import java.math.BigDecimal
import java.util.concurrent.ConcurrentHashMap

/**
 * A net holding built from the fill stream: signed [quantity] (positive long, negative short —
 * zero is a flat symbol that has traded), and the [lastPrice]/[lastTimeMillis] it marked at.
 */
data class Position(
    val symbol: String,
    val quantity: Long,
    val lastPrice: BigDecimal,
    val lastTimeMillis: Long,
)

/**
 * Net position per symbol: the in-memory mirror of the [PositionStore]'s truth. Rows arrive
 * only from committed store transactions — [put] after each applied fill, [restore] from the
 * store at startup — so the book can never run ahead of the database. Thread-safe: the consumer
 * thread writes, web threads read.
 */
class PositionBook {
    private val positions = ConcurrentHashMap<String, Position>()

    /** Replaces the symbol's position with the row the store committed. */
    fun put(position: Position) {
        positions[position.symbol] = position
    }

    fun positionOf(symbol: String): Position? = positions[symbol]

    /** Every symbol that has traded, stably ordered for rendering. */
    fun all(): List<Position> = positions.values.sortedBy { it.symbol }

    /** Loads persisted positions, replacing any in-memory state — startup only. */
    fun restore(persisted: List<Position>) {
        positions.clear()
        persisted.forEach { positions[it.symbol] = it }
    }
}
