package com.damianhoward.tradingsystem.limits

import com.damianhoward.tradingsystem.consume.ConsumerProgress
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class LimitsReportTest {
    private val limits = RiskLimits(50, BigDecimal("5000"))

    @Test
    fun `an untouched checker serialises to an explicitly empty state`() {
        assertEquals(
            """{"maxPosition":50,"maxNotional":5000,"symbols":[],"events":[],"malformed":0,"progress":null}""",
            LimitsReport(limits, emptyList(), emptyList(), 0).toJson(),
        )
    }

    @Test
    fun `exposures and events serialise with exact decimals`() {
        val symbol =
            SymbolLimits(
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
                """"malformed":2,"progress":{"offset":41,"fillTs":1720620000000}}""",
            LimitsReport(
                limits,
                listOf(symbol),
                listOf(event),
                2,
                ConsumerProgress(41, 1720620000000),
            ).toJson(),
        )
    }

    @Test
    fun `symbols are JSON-escaped`() {
        val symbol =
            SymbolLimits(
                symbol = """A"B\C""",
                netQuantity = 1,
                lastPrice = BigDecimal.ONE,
                notional = BigDecimal.ONE,
                positionUtilisation = BigDecimal("0.0200"),
                notionalUtilisation = BigDecimal("0.0002"),
                breached = false,
            )
        val event = BreachEvent("""A"B\C""", LimitKind.NOTIONAL, false, BigDecimal.ONE, BigDecimal.ONE, 0)

        val json = LimitsReport(limits, listOf(symbol), listOf(event), 0).toJson()

        assertTrue(json.contains(""""symbol":"A\"B\\C""""), json)
    }
}
