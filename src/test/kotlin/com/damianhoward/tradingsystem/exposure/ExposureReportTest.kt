package com.damianhoward.tradingsystem.exposure

import com.damianhoward.tradingsystem.consume.ConsumerProgress
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class ExposureReportTest {
    private val limits = RiskLimits(50, BigDecimal("5000"))

    @Test
    fun `an untouched detector serialises to an explicitly empty state`() {
        assertEquals(
            """{"maxPosition":50,"maxNotional":5000,"symbols":[],"events":[],"malformed":0,"breaches":0,"progress":null}""",
            ExposureReport(limits, emptyList(), emptyList(), 0).toJson(),
        )
    }

    @Test
    fun `exposures and events serialise with exact decimals`() {
        val symbol =
            SymbolExposure(
                symbol = "SIM",
                netQuantity = 7,
                lastPrice = BigDecimal("101.00000000"),
                notional = BigDecimal("707.00000000"),
                positionUtilisation = BigDecimal("0.1400"),
                notionalUtilisation = BigDecimal("0.1414"),
                breached = false,
            )
        val event = BreachEvent("SIM", LimitKind.POSITION, true, BigDecimal("55"), BigDecimal("50"), 1720620000000)

        assertEquals(
            """{"maxPosition":50,"maxNotional":5000,"symbols":[""" +
                """{"symbol":"SIM","netQuantity":7,"lastPrice":101.00000000,"notional":707.00000000,""" +
                """"positionUtilisation":0.1400,"notionalUtilisation":0.1414,"breached":false}],""" +
                """"events":[{"symbol":"SIM","kind":"POSITION","breached":true,"value":55,"limit":50,"ts":1720620000000}],""" +
                """"malformed":2,"breaches":1,"progress":{"offset":41,"fillTs":1720620000000}}""",
            ExposureReport(
                limits,
                listOf(symbol),
                listOf(event),
                malformed = 2,
                breaches = 1,
                progress = ConsumerProgress(41, 1720620000000),
            ).toJson(),
        )
    }

    @Test
    fun `the worst utilisation is the maximum across symbols, per ceiling`() {
        val report =
            ExposureReport(
                limits,
                listOf(
                    exposureAt(symbol = "AAA", position = "0.9000", notional = "0.2000"),
                    exposureAt(symbol = "BBB", position = "0.3000", notional = "1.4000"),
                ),
                emptyList(),
                0,
            )

        assertEquals(BigDecimal("0.9000"), report.worstUtilisation(LimitKind.POSITION))
        assertEquals(BigDecimal("1.4000"), report.worstUtilisation(LimitKind.NOTIONAL))
    }

    @Test
    fun `nothing to measure publishes no utilisation rather than a zero`() {
        val empty = ExposureReport(limits, emptyList(), emptyList(), 0)

        assertNull(empty.worstUtilisation(LimitKind.POSITION))
        assertNull(empty.worstUtilisation(LimitKind.NOTIONAL))
        assertEquals(0, empty.breachedSymbols)
    }

    @Test
    fun `breached symbols are counted over either ceiling`() {
        val report =
            ExposureReport(
                limits,
                listOf(
                    exposureAt(symbol = "AAA", position = "1.2000", notional = "0.1000", breached = true),
                    exposureAt(symbol = "BBB", position = "0.1000", notional = "0.1000"),
                    exposureAt(symbol = "CCC", position = "0.1000", notional = "3.0000", breached = true),
                ),
                emptyList(),
                0,
            )

        assertEquals(2, report.breachedSymbols)
    }

    private fun exposureAt(
        symbol: String,
        position: String,
        notional: String,
        breached: Boolean = false,
    ) = SymbolExposure(
        symbol = symbol,
        netQuantity = 1,
        lastPrice = BigDecimal.ONE,
        notional = BigDecimal.ONE,
        positionUtilisation = BigDecimal(position),
        notionalUtilisation = BigDecimal(notional),
        breached = breached,
    )

    @Test
    fun `symbols are JSON-escaped`() {
        val symbol =
            SymbolExposure(
                symbol = """A"B\C""",
                netQuantity = 1,
                lastPrice = BigDecimal.ONE,
                notional = BigDecimal.ONE,
                positionUtilisation = BigDecimal("0.0200"),
                notionalUtilisation = BigDecimal("0.0002"),
                breached = false,
            )
        val event = BreachEvent("""A"B\C""", LimitKind.NOTIONAL, false, BigDecimal.ONE, BigDecimal.ONE, 0)

        val json = ExposureReport(limits, listOf(symbol), listOf(event), 0).toJson()

        assertTrue(json.contains(""""symbol":"A\"B\\C""""), json)
    }
}
