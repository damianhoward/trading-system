package com.damianhoward.tradingsystem.exposure

import com.damianhoward.tradingsystem.consume.ConsumerProgress
import java.math.BigDecimal

/** Which ceiling a breach event refers to. */
enum class LimitKind { POSITION, NOTIONAL }

/**
 * One transition of a symbol's standing against a single limit: [breached] is true when the
 * limit was crossed, false when exposure dropped back under. [timeMillis] is the fill's own
 * timestamp, so replaying the stream rebuilds the same events.
 */
data class BreachEvent(
    val symbol: String,
    val kind: LimitKind,
    val breached: Boolean,
    val value: BigDecimal,
    val limit: BigDecimal,
    val timeMillis: Long,
)

/** A symbol's current exposure against both limits. Utilisations are 4-dp fractions of the limit. */
data class SymbolExposure(
    val symbol: String,
    val netQuantity: Long,
    val lastPrice: BigDecimal,
    val notional: BigDecimal,
    val positionUtilisation: BigDecimal,
    val notionalUtilisation: BigDecimal,
    val breached: Boolean,
) {
    fun utilisation(kind: LimitKind): BigDecimal =
        when (kind) {
            LimitKind.POSITION -> positionUtilisation
            LimitKind.NOTIONAL -> notionalUtilisation
        }
}

/**
 * The detector's whole view at one moment: per-symbol exposures, the bounded breach/clear history
 * (newest first), how many malformed records were skipped, and how far along the stream this view
 * has read ([progress], null before the first fill). [toJson] follows the dashboard's wire
 * convention — one line, exact decimals as plain JSON numbers.
 */
data class ExposureReport(
    val limits: RiskLimits,
    val symbols: List<SymbolExposure>,
    val events: List<BreachEvent>,
    val malformed: Long,
    /** Breaches counted since start. Monotonic, unlike [events], which the deque evicts. */
    val breaches: Long = 0,
    val progress: ConsumerProgress? = null,
) {
    val breachedSymbols: Int get() = symbols.count { it.breached }

    /**
     * The worst utilisation across symbols for one ceiling, null with nothing to measure. Above 1
     * means breached.
     *
     * A maximum rather than a series per symbol: labelled by symbol, cardinality would grow with
     * what trades rather than with what is configured.
     */
    fun worstUtilisation(kind: LimitKind): BigDecimal? = symbols.map { it.utilisation(kind) }.maxOrNull()

    fun toJson(): String =
        """{"maxPosition":${limits.maxAbsPosition},"maxNotional":${limits.maxNotional.toPlainString()},""" +
            """"symbols":[${symbols.joinToString(",", transform = ::symbolJson)}],""" +
            """"events":[${events.joinToString(",", transform = ::eventJson)}],"malformed":$malformed,""" +
            """"breaches":$breaches,""" +
            """"progress":${progressJson(progress)}}"""

    private fun progressJson(p: ConsumerProgress?): String =
        if (p == null) "null" else """{"offset":${p.offset},"fillTs":${p.fillTimeMillis}}"""

    private fun symbolJson(s: SymbolExposure): String =
        """{"symbol":${quote(s.symbol)},"netQuantity":${s.netQuantity},"lastPrice":${s.lastPrice.toPlainString()},""" +
            """"notional":${s.notional.toPlainString()},"positionUtilisation":${s.positionUtilisation.toPlainString()},""" +
            """"notionalUtilisation":${s.notionalUtilisation.toPlainString()},"breached":${s.breached}}"""

    private fun eventJson(e: BreachEvent): String =
        """{"symbol":${quote(e.symbol)},"kind":"${e.kind}","breached":${e.breached},""" +
            """"value":${e.value.toPlainString()},"limit":${e.limit.toPlainString()},"ts":${e.timeMillis}}"""

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
