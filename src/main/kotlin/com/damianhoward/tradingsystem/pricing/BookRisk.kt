package com.damianhoward.tradingsystem.pricing

import com.damianhoward.riskengine.report.RiskReport
import java.math.BigDecimal

/** One symbol's validated single-underlier report, beside the open its day PnL measures from. */
data class SymbolRisk(
    val symbol: String,
    val openPrice: BigDecimal?,
    val report: RiskReport,
)

/**
 * The whole book's risk: every position gets its own report, plus the sums that are honest to
 * sum. Valuation, gross notional, and day PnL are currency amounts and add; Greeks and VaR stay
 * per-symbol on purpose — summing share-count deltas across underliers, or taking a quantile
 * across independently-scenarioed symbols, would label numbers the library's single-underlier
 * market model does not price.
 */
data class BookRisk(
    val symbols: List<SymbolRisk>,
) {
    val valuation: BigDecimal = symbols.sumOf { it.report.valuation.amount }

    /** Σ|per-symbol valuation| — for a cash-equity book each term is |quantity × spot|. */
    val grossNotional: BigDecimal =
        symbols.sumOf {
            it.report.valuation.amount
                .abs()
        }

    /** Sum of the day PnL of every symbol that has traded today, or null when none has. */
    val dayPnl: BigDecimal? =
        symbols
            .mapNotNull {
                it.report.pnl
                    ?.actual
                    ?.amount
            }.takeIf { it.isNotEmpty() }
            ?.reduce(BigDecimal::add)

    fun toJson(): String =
        """{"valuation":${valuation.toPlainString()},"grossNotional":${grossNotional.toPlainString()},""" +
            """"dayPnl":${dayPnl?.toPlainString() ?: "null"},""" +
            """"symbols":[${symbols.joinToString(",", transform = ::symbolJson)}]}"""

    private fun symbolJson(s: SymbolRisk): String =
        """{"symbol":${quote(s.symbol)},"openPrice":${s.openPrice?.toPlainString() ?: "null"},""" +
            """"report":${s.report.toJson()}}"""

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
