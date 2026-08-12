package com.damianhoward.tradingsystem.limits

import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.consume.Fill
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicInteger

class LimitsCheckerTest {
    private val limits = RiskLimits(maxAbsPosition = 10, maxNotional = BigDecimal("2000"))
    private val checker = LimitsChecker(limits)

    private fun record(json: String): ConsumerRecord<String, String> = ConsumerRecord("orderbook.fills", 0, 0, "SIM", json)

    private fun fill(
        size: Long,
        aggressor: String = "BID",
        price: String = "101.00000000",
        ts: Long = 1000,
    ): ConsumerRecord<String, String> =
        record(
            """{"v":1,"symbol":"SIM","price":"$price","size":$size,""" +
                """"makerOrderId":1,"takerOrderId":2,"aggressor":"$aggressor","ts":$ts}""",
        )

    @Test
    fun `a fill under both limits reports exposure with no events`() {
        checker.handle(fill(size = 4))

        val symbol = checker.report().symbols.single()
        assertEquals(4, symbol.netQuantity)
        assertEquals(BigDecimal("101.00000000"), symbol.lastPrice)
        assertEquals(BigDecimal("404.00000000"), symbol.notional)
        assertEquals(BigDecimal("0.4000"), symbol.positionUtilisation)
        assertEquals(BigDecimal("0.2020"), symbol.notionalUtilisation)
        assertFalse(symbol.breached)
        assertTrue(checker.report().events.isEmpty())
    }

    @Test
    fun `crossing the position limit flags a breach stamped with the fill's own time`() {
        checker.handle(fill(size = 6))
        checker.handle(fill(size = 6, ts = 2000))

        val report = checker.report()
        assertTrue(report.symbols.single().breached)
        val event = report.events.single()
        assertEquals(LimitKind.POSITION, event.kind)
        assertTrue(event.breached)
        assertEquals(BigDecimal("12"), event.value)
        assertEquals(BigDecimal("10"), event.limit)
        assertEquals(2000, event.timeMillis)
    }

    @Test
    fun `dropping back under the limit clears the breach with a second event`() {
        checker.handle(fill(size = 12))
        checker.handle(fill(size = 5, aggressor = "OFFER", ts = 3000))

        val report = checker.report()
        assertFalse(report.symbols.single().breached)
        assertEquals(listOf(false, true), report.events.map { it.breached }, "newest first: the clear, then the breach")
        assertEquals(3000, report.events.first().timeMillis)
    }

    @Test
    fun `notional can breach while the position count stays legal`() {
        checker.handle(fill(size = 5, price = "450.00"))

        val report = checker.report()
        assertTrue(report.symbols.single().breached)
        val event = report.events.single()
        assertEquals(LimitKind.NOTIONAL, event.kind)
        assertEquals(BigDecimal("2250.00"), event.value)
        assertEquals(BigDecimal("2000"), event.limit)
    }

    @Test
    fun `a short position breaches on its absolute size`() {
        checker.handle(fill(size = 12, aggressor = "OFFER"))

        val report = checker.report()
        assertEquals(-12, report.symbols.single().netQuantity)
        assertEquals(LimitKind.POSITION, report.events.single().kind)
        assertTrue(report.events.single().breached)
    }

    @Test
    fun `a further fill while already breached adds no duplicate event`() {
        checker.handle(fill(size = 12))
        checker.handle(fill(size = 1, ts = 2000))

        assertEquals(1, checker.report().events.size, "an event marks the transition, not every over-limit fill")
    }

    @Test
    fun `a malformed record is counted and skipped and the stream keeps moving`() {
        checker.handle(record("""{"not":"a fill"}"""))
        checker.handle(fill(size = 4))

        val report = checker.report()
        assertEquals(1, report.malformed)
        assertEquals(4, report.symbols.single().netQuantity)
        assertTrue(report.events.isEmpty())
    }

    @Test
    fun `the event history is bounded, keeping the newest`() {
        val tight = LimitsChecker(RiskLimits(maxAbsPosition = 1, maxNotional = BigDecimal("1000000")))
        repeat(11) { cycle ->
            tight.handle(fill(size = 2, ts = (cycle * 2).toLong()))
            tight.handle(fill(size = 2, aggressor = "OFFER", ts = (cycle * 2 + 1).toLong()))
        }

        val events = tight.report().events
        assertEquals(LimitsChecker.MAX_EVENTS, events.size, "22 transitions happened; only the newest 20 are kept")
        assertEquals(21, events.first().timeMillis)
        assertEquals(2, events.last().timeMillis)
    }

    @Test
    fun `the change listener fires for applied and malformed records alike`() {
        val changes = AtomicInteger()
        checker.onChange { changes.incrementAndGet() }

        checker.handle(fill(size = 1))
        checker.handle(record("not even json"))

        assertEquals(2, changes.get())
    }

    @Test
    fun `a report is an immutable copy of the state at that moment`() {
        checker.handle(fill(size = 4))
        val before = checker.report()

        checker.handle(fill(size = 12, ts = 2000))

        assertEquals(4, before.symbols.single().netQuantity)
        assertTrue(before.events.isEmpty())
        assertEquals(
            16,
            checker
                .report()
                .symbols
                .single()
                .netQuantity,
        )
    }

    @Test
    fun `reports stay internally consistent while the consumer thread writes`() {
        val writer =
            Thread {
                repeat(500) { checker.handle(fill(size = 1, ts = it.toLong())) }
            }.apply { start() }
        repeat(200) {
            val symbol = checker.report().symbols.singleOrNull() ?: return@repeat
            assertEquals(
                BigDecimal(symbol.netQuantity).abs() * symbol.lastPrice,
                symbol.notional,
                "notional must match the quantity and price captured in the same report",
            )
        }
        writer.join(10_000)
        assertEquals(
            500,
            checker
                .report()
                .symbols
                .single()
                .netQuantity,
        )
    }

    @Test
    fun `warm rebuilds the same state a live replay of those fills would`() {
        val live = LimitsChecker(limits)
        live.handle(fill(size = 12, ts = 1000))
        live.handle(fill(size = 5, aggressor = "OFFER", ts = 2000))

        val warmed = LimitsChecker(limits)
        warmed.warm(
            listOf(
                parsedFill(size = 12, ts = 1000),
                parsedFill(size = 5, aggressor = "OFFER", ts = 2000),
            ),
            ConsumerProgress(1, 2000),
        )

        val liveReport = live.report()
        val warmedReport = warmed.report()
        assertEquals(liveReport.symbols, warmedReport.symbols, "exposures match a live consumption of the same fills")
        assertEquals(liveReport.events, warmedReport.events, "breach history rebuilds too — events carry fill time")
        assertEquals(1, warmedReport.progress?.offset)
    }

    @Test
    fun `progress tracks the last handled record's offset and fill time`() {
        assertEquals(null, checker.report().progress, "no progress before the first fill")
        checker.handle(
            ConsumerRecord(
                "orderbook.fills",
                0,
                41,
                "SIM",
                """{"v":1,"symbol":"SIM","price":"101.00000000","size":4,""" +
                    """"makerOrderId":1,"takerOrderId":2,"aggressor":"BID","ts":7000}""",
            ),
        )

        val progress = checker.report().progress
        assertEquals(41, progress?.offset)
        assertEquals(7000, progress?.fillTimeMillis)
    }

    private fun parsedFill(
        size: Long,
        aggressor: String = "BID",
        ts: Long = 1000,
    ) = Fill.parse(
        """{"v":1,"symbol":"SIM","price":"101.00000000","size":$size,""" +
            """"makerOrderId":1,"takerOrderId":2,"aggressor":"$aggressor","ts":$ts}""",
    )

    @Test
    fun `limits must be positive`() {
        assertThrows(IllegalArgumentException::class.java) { RiskLimits(0, BigDecimal.ONE) }
        assertThrows(IllegalArgumentException::class.java) { RiskLimits(1, BigDecimal.ZERO) }
        assertThrows(IllegalArgumentException::class.java) { RiskLimits(-5, BigDecimal.ONE) }
    }
}
