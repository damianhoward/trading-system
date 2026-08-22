package com.damianhoward.tradingsystem.capture

import com.damianhoward.orderbook.model.Side
import com.damianhoward.riskengine.report.RiskReportAssembler
import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.consume.Fill
import com.damianhoward.tradingsystem.consume.FillSource
import com.damianhoward.tradingsystem.exposure.ExposureReport
import com.damianhoward.tradingsystem.exposure.RiskLimits
import com.damianhoward.tradingsystem.position.Ledger
import com.damianhoward.tradingsystem.position.LedgerSnapshot
import com.damianhoward.tradingsystem.position.Position
import com.damianhoward.tradingsystem.position.PositionBook
import com.damianhoward.tradingsystem.position.PositionStore
import com.damianhoward.tradingsystem.position.RecordOutcome
import com.damianhoward.tradingsystem.position.SymbolTotals
import com.damianhoward.tradingsystem.pricing.MarketAssumptions
import com.damianhoward.tradingsystem.pricing.RiskGateway
import com.damianhoward.tradingsystem.web.Broadcaster
import com.sun.net.httpserver.HttpExchange
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class TradeCaptureTest {
    /** Ledger semantics in memory: unique source coordinates, delta-derived positions. */
    private class InMemoryStore : PositionStore {
        val ledger = LinkedHashMap<FillSource, Fill>()
        val positions = LinkedHashMap<String, Position>()
        var failNext = false

        override fun record(
            fill: Fill,
            source: FillSource,
        ): RecordOutcome {
            if (failNext) {
                failNext = false
                throw IllegalStateException("database blinked")
            }
            if (source in ledger) return RecordOutcome.Duplicate
            ledger[source] = fill
            val updated =
                Position(
                    symbol = fill.symbol,
                    quantity = (positions[fill.symbol]?.quantity ?: 0L) + fill.signedSize,
                    lastPrice = fill.price,
                    lastTimeMillis = fill.timeMillis,
                )
            positions[fill.symbol] = updated
            return RecordOutcome.Applied(updated)
        }

        override fun loadAll(): List<Position> = positions.values.sortedBy { it.symbol }

        override fun loadLedger(topic: String): Ledger = Ledger(ledger.values.toList(), emptyMap())

        // Derived from both maps rather than asserted equal, so this double can express a
        // divergence at all — a stand-in that reports agreement by construction could never fail
        // the check it stands in for.
        override fun ledgerSnapshot(topic: String): LedgerSnapshot =
            LedgerSnapshot(
                highWaterOffset = ledger.keys.filter { it.topic == topic }.maxOfOrNull { it.offset },
                totals =
                    (ledger.values.map { it.symbol } + positions.keys).distinct().sorted().map { symbol ->
                        SymbolTotals(
                            symbol = symbol,
                            ledgerQuantity = ledger.values.filter { it.symbol == symbol }.sumOf { it.signedSize },
                            positionQuantity = positions[symbol]?.quantity ?: 0L,
                        )
                    },
            )

        override fun ping(): Boolean = true
    }

    private class RecordingBroadcaster : Broadcaster {
        val frames = mutableListOf<String>()

        override fun startHeartbeat(periodSeconds: Long) {}

        override fun broadcast(json: String) {
            frames.add(json)
        }

        override fun stream(
            exchange: HttpExchange,
            initialJson: String,
        ) = throw UnsupportedOperationException("not used in this test")
    }

    private val store = InMemoryStore()
    private val broadcaster = RecordingBroadcaster()
    private val book = PositionBook()
    private val capture =
        TradeCapture(
            book = book,
            store = store,
            risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
            broadcaster = broadcaster,
            exposureView = { ExposureReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0) },
        )

    private fun fill(
        side: Side = Side.BID,
        size: Long = 5,
        price: String = "100.00",
        ts: Long = 1000,
        symbol: String = "SIM",
    ) = Fill(symbol, BigDecimal(price), size, 1, 2, side, ts)

    private fun source(offset: Long = 7) = FillSource("orderbook.fills", 0, offset)

    @Test
    fun `a fill is recorded, mirrored into the book, and broadcast as a fresh snapshot`() {
        capture.onFill(fill(), source())

        assertEquals(5, store.positions["SIM"]?.quantity)
        assertEquals(5, book.positionOf("SIM")?.quantity)
        val frame = broadcaster.frames.single()
        assertTrue(frame.contains(""""quantity":5"""), frame)
        assertTrue(frame.contains(""""report":{"""), "the broadcast snapshot carries the repriced report")
        assertTrue(frame.contains(""""exposure":{"""), "the broadcast snapshot carries the limits view")
    }

    @Test
    fun `a retried fill cannot move the position twice`() {
        store.failNext = true
        assertThrows(IllegalStateException::class.java) { capture.onFill(fill(), source()) }
        assertNull(book.positionOf("SIM"), "a failed transaction must leave memory untouched")
        assertTrue(broadcaster.frames.isEmpty(), "a fill that didn't persist must not be announced")

        // The consumer's retry replays the same record; one application is the invariant.
        capture.onFill(fill(), source())
        assertEquals(5, book.positionOf("SIM")?.quantity)
        assertEquals(5, store.positions["SIM"]?.quantity)
    }

    @Test
    fun `a redelivered fill is dropped by the ledger, not applied again`() {
        capture.onFill(fill(), source(offset = 7))
        capture.onFill(fill(), source(offset = 7))

        assertEquals(5, book.positionOf("SIM")?.quantity, "the duplicate must not double the position")
        assertEquals(1, broadcaster.frames.size, "a dropped replay is not a state change to announce")
        assertEquals(1, capture.duplicates)
        assertTrue(capture.snapshot().toJson().contains(""""duplicatesDropped":1"""))
    }

    @Test
    fun `the snapshot carries the dead-letter count for the dashboard's operator flag`() {
        val withDeadLetters =
            TradeCapture(
                book = book,
                store = store,
                risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
                broadcaster = broadcaster,
                exposureView = { ExposureReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0) },
                deadLetters = { 3 },
            )

        assertTrue(withDeadLetters.snapshot().toJson().contains(""""deadLetters":3"""))
    }

    @Test
    fun `day PnL measures from the day's first fill via the shared opens`() {
        capture.onFill(fill(price = "100.00"), source(offset = 1))
        capture.onFill(fill(price = "103.00", ts = 2000), source(offset = 2))

        val symbol =
            capture
                .snapshot()
                .book!!
                .symbols
                .single()
        assertEquals(BigDecimal("100.00"), symbol.openPrice, "the open is the day's first fill, not the latest")
        assertTrue(symbol.report.pnl != null, "with an open mark the report attributes day PnL")
    }

    @Test
    fun `every position is valued — the book covers all symbols, not the first`() {
        capture.onFill(fill(price = "100.00"), source(offset = 1))
        capture.onFill(fill(price = "300.00", symbol = "AAPL", ts = 2000), source(offset = 2))

        val book = capture.snapshot().book!!
        assertEquals(listOf("AAPL", "SIM"), book.symbols.map { it.symbol })
        // 5 shares at each symbol's own mark: 5×300 + 5×100.
        assertEquals(0, BigDecimal("2000").compareTo(book.valuation), book.valuation.toPlainString())
    }

    @Test
    fun `before any fill the snapshot is explicitly empty`() {
        val snapshot = capture.snapshot()
        assertTrue(snapshot.positions.isEmpty())
        assertNull(snapshot.book)
        assertTrue(broadcaster.frames.isEmpty(), "nothing to push until something trades")
    }

    @Test
    fun `snapshot progress starts at the warmed ledger mark and follows applied fills`() {
        val warmed =
            TradeCapture(
                book = book,
                store = store,
                risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
                broadcaster = broadcaster,
                exposureView = { ExposureReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0) },
                initialProgress = ConsumerProgress(41, 900),
            )
        assertEquals(ConsumerProgress(41, 900), warmed.snapshot().positionsProgress)

        warmed.onFill(fill(ts = 1500), source(offset = 42))
        assertEquals(ConsumerProgress(42, 1500), warmed.snapshot().positionsProgress)
    }

    @Test
    fun `a replayed record still advances the reported stream position`() {
        capture.onFill(fill(), source(offset = 7))
        capture.onFill(fill(ts = 2000), source(offset = 7))

        val progress = capture.snapshot().positionsProgress
        assertEquals(7, progress?.offset)
        assertFalse(broadcaster.frames.size > 1)
    }
}
