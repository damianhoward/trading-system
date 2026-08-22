package com.damianhoward.tradingsystem.view

import com.damianhoward.riskengine.report.RiskReportAssembler
import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.exposure.ExposureReport
import com.damianhoward.tradingsystem.exposure.RiskLimits
import com.damianhoward.tradingsystem.position.Position
import com.damianhoward.tradingsystem.pricing.MarketAssumptions
import com.damianhoward.tradingsystem.pricing.RiskGateway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class DashboardSnapshotTest {
    private val emptyExposure = ExposureReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0)
    private val gateway = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default())

    @Test
    fun `an untraded system serialises to an explicitly empty state`() {
        assertEquals(
            """{"v":2,"positions":[],"book":null,""" +
                """"exposure":{"maxPosition":50,"maxNotional":5000,"symbols":[],"events":[],""" +
                """"malformed":0,"breaches":0,"progress":null},""" +
                """"sync":{"positions":null,"exposure":null,"coherent":true,"duplicatesDropped":0,"deadLetters":0}}""",
            DashboardSnapshot(emptyList(), null, emptyExposure).toJson(),
        )
    }

    @Test
    fun `positions serialise with exact prices and the book embeds each symbol's report`() {
        val positions =
            listOf(
                Position("AAPL", 10, BigDecimal("300.00000000"), 1720620000000),
                Position("SIM", -42, BigDecimal("101.00000000"), 1720620000000),
            )
        val book = gateway.bookReport(positions) { BigDecimal("100.00") }

        val json = DashboardSnapshot(positions, book, emptyExposure).toJson()

        assertTrue(
            json.startsWith(
                """{"v":2,"positions":[{"symbol":"AAPL","quantity":10,""" +
                    """"lastPrice":300.00000000,"lastTimeMillis":1720620000000},""",
            ),
            json,
        )
        assertTrue(json.contains(""""book":{"valuation":"""), json)
        assertTrue(json.contains(""""symbols":[{"symbol":"AAPL","openPrice":100.00,"report":{"""), json)
        assertTrue(json.contains(""""symbol":"SIM""""), "every position gets a report")
        assertTrue(json.contains(""""greeks":{"""), "the report JSON is embedded as-is")
        assertTrue(json.contains(""""pnl":{"""), "a session-open mark yields day PnL")
        assertTrue(json.contains(""""exposure":{"maxPosition":50,"""), "the limits JSON is embedded as-is")
    }

    @Test
    fun `matching stream positions are coherent, diverged ones are flagged`() {
        val limitsAt = emptyExposure.copy(progress = ConsumerProgress(7, 1000))
        val together =
            DashboardSnapshot(emptyList(), null, limitsAt, ConsumerProgress(7, 1000), duplicates = 2, deadLetters = 1)
        assertTrue(together.coherent)
        assertTrue(
            together.toJson().contains(
                """"sync":{"positions":{"offset":7,"fillTs":1000},"exposure":{"offset":7,"fillTs":1000},""" +
                    """"coherent":true,"duplicatesDropped":2,"deadLetters":1}""",
            ),
            together.toJson(),
        )

        val apart = DashboardSnapshot(emptyList(), null, limitsAt, ConsumerProgress(9, 3000))
        assertFalse(apart.coherent, "the two views describe different stream positions")
        assertTrue(apart.toJson().contains(""""coherent":false"""))
    }

    @Test
    fun `one warmed view beside one empty view is not coherent`() {
        val snapshot = DashboardSnapshot(emptyList(), null, emptyExposure, ConsumerProgress(7, 1000))
        assertFalse(snapshot.coherent)
    }

    @Test
    fun `symbols are JSON-escaped`() {
        val position = Position("""A"B\C""", 1, BigDecimal("1"), 0)
        val json = DashboardSnapshot(listOf(position), null, emptyExposure).toJson()
        assertTrue(json.contains(""""symbol":"A\"B\\C""""), json)
    }
}
