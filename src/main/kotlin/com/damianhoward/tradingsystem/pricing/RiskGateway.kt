package com.damianhoward.tradingsystem.pricing

import com.damianhoward.riskengine.model.Equity
import com.damianhoward.riskengine.model.MarketData
import com.damianhoward.riskengine.model.Money
import com.damianhoward.riskengine.model.Portfolio
import com.damianhoward.riskengine.report.RiskReport
import com.damianhoward.riskengine.report.RiskReportAssembler
import com.damianhoward.tradingsystem.position.Position
import java.math.BigDecimal
import java.util.Random
import com.damianhoward.riskengine.model.Position as RiskPosition

/**
 * The market inputs the tape cannot supply: volatility, rates, the scenario set VaR runs over,
 * and the report's confidence. An explicit named value injected into [RiskGateway], not literals
 * buried in the pipeline. [timeToExpiry] parameterises the market for derivatives; the current
 * equity-only book prices independently of it, but [MarketData] carries it for the option slice
 * to come.
 */
data class MarketAssumptions(
    val volatility: Double,
    val riskFreeRate: Double,
    val dividendYield: Double,
    val timeToExpiry: Double,
    val scenarioReturns: List<Double>,
    val confidence: Double,
) {
    companion object {
        /**
         * 20-vol, 4% rates, and 250 trading days of ~1.5%-vol zero-mean daily moves from a fixed
         * seed, so the report is reproducible across runs and restarts.
         */
        fun default(): MarketAssumptions {
            val rng = Random(42)
            return MarketAssumptions(
                volatility = 0.20,
                riskFreeRate = 0.04,
                dividendYield = 0.0,
                timeToExpiry = 0.5,
                scenarioReturns = List(250) { rng.nextGaussian() * 0.015 },
                confidence = 0.99,
            )
        }
    }
}

/**
 * The bridge into risk-engine: builds a [Portfolio] per position, marks it at the symbol's last
 * traded price, and asks the library for the day's report — valuation, Greeks, VaR/ES both ways,
 * and (when a session-open mark exists) the day's PnL attribution. A direct library call, not
 * another Kafka hop; every number comes from risk-engine's validated calculators.
 */
class RiskGateway(
    private val assembler: RiskReportAssembler,
    private val assumptions: MarketAssumptions,
) {
    /**
     * One report per position — the library's market model is single-underlier, so each symbol
     * is priced in its own market and [BookRisk] carries only the sums that survive that model.
     * Null before anything has traded (no mark to price against).
     */
    fun bookReport(
        positions: List<Position>,
        openFor: (String) -> BigDecimal?,
    ): BookRisk? =
        positions
            .map { position ->
                val open = openFor(position.symbol)
                SymbolRisk(position.symbol, open, checkNotNull(report(position, open)))
            }.takeIf { it.isNotEmpty() }
            ?.let(::BookRisk)

    /**
     * The report over [position], or null before anything has traded (no mark to price against).
     * [openPrice] is the session-open mark; when present the report carries day PnL from it.
     */
    fun report(
        position: Position?,
        openPrice: BigDecimal?,
    ): RiskReport? {
        val marked = position ?: return null
        val portfolio =
            if (marked.quantity == 0L) {
                Portfolio(emptyList()) // a flat book values to zero; the report still carries VaR of nothing
            } else {
                Portfolio.of(RiskPosition(Equity, marked.quantity.toDouble()))
            }
        val market =
            MarketData(
                spot = Money(marked.lastPrice),
                volatility = assumptions.volatility,
                riskFreeRate = assumptions.riskFreeRate,
                dividendYield = assumptions.dividendYield,
                timeToExpiry = assumptions.timeToExpiry,
            )
        // The prior mark sits one day behind the current one, so time runs forward to today.
        val priorMarket =
            openPrice?.let {
                market.copy(spot = Money(it), timeToExpiry = assumptions.timeToExpiry + ONE_DAY)
            }
        return assembler.assemble(portfolio, market, assumptions.scenarioReturns, assumptions.confidence, priorMarket)
    }

    companion object {
        private const val ONE_DAY = 1.0 / 365
    }
}
