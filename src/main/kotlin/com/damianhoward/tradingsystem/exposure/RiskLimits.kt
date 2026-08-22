package com.damianhoward.tradingsystem.exposure

import java.math.BigDecimal

/**
 * The exposure ceilings the limits consumer checks every fill against, per symbol: absolute net
 * position and notional (|net quantity| × last price).
 */
data class RiskLimits(
    val maxAbsPosition: Long,
    val maxNotional: BigDecimal,
) {
    init {
        require(maxAbsPosition > 0) { "maxAbsPosition must be positive, got $maxAbsPosition" }
        require(maxNotional.signum() > 0) { "maxNotional must be positive, got $maxNotional" }
    }
}
