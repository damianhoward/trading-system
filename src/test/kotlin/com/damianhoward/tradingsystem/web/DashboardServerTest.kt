package com.damianhoward.tradingsystem.web

import com.damianhoward.orderbook.model.Side
import com.damianhoward.riskengine.report.RiskReportAssembler
import com.damianhoward.tradingsystem.capture.TradeCapture
import com.damianhoward.tradingsystem.consume.ConsumerHealth
import com.damianhoward.tradingsystem.consume.Fill
import com.damianhoward.tradingsystem.consume.FillSource
import com.damianhoward.tradingsystem.health.Readiness
import com.damianhoward.tradingsystem.limits.LimitsReport
import com.damianhoward.tradingsystem.limits.RiskLimits
import com.damianhoward.tradingsystem.position.Ledger
import com.damianhoward.tradingsystem.position.Position
import com.damianhoward.tradingsystem.position.PositionBook
import com.damianhoward.tradingsystem.position.PositionStore
import com.damianhoward.tradingsystem.position.Reconciliation
import com.damianhoward.tradingsystem.position.RecordOutcome
import com.damianhoward.tradingsystem.position.SymbolTotals
import com.damianhoward.tradingsystem.pricing.MarketAssumptions
import com.damianhoward.tradingsystem.pricing.RiskGateway
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.BufferedReader
import java.io.IOException
import java.math.BigDecimal
import java.net.HttpURLConnection
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** The server against real loopback HTTP: routing, content types, the state JSON, readiness, and the SSE push. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DashboardServerTest {
    private class InMemoryStore : PositionStore {
        private val positions = HashMap<String, Position>()

        override fun record(
            fill: Fill,
            source: FillSource,
        ): RecordOutcome {
            val updated =
                Position(fill.symbol, (positions[fill.symbol]?.quantity ?: 0L) + fill.signedSize, fill.price, fill.timeMillis)
            positions[fill.symbol] = updated
            return RecordOutcome.Applied(updated)
        }

        override fun loadAll(): List<Position> = positions.values.sortedBy { it.symbol }

        override fun loadLedger(topic: String): Ledger = Ledger(emptyList(), emptyMap())

        override fun symbolTotals(): List<SymbolTotals> = positions.values.map { SymbolTotals(it.symbol, it.quantity, it.quantity) }

        override fun ping(): Boolean = true
    }

    private val broadcaster = SseBroadcaster()
    private val capture =
        TradeCapture(
            book = PositionBook(),
            store = InMemoryStore(),
            risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
            broadcaster = broadcaster,
            limitsView = { LimitsReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0) },
        )
    private val consumerHealth = ConsumerHealth("test-consumer").apply { started() }
    private val readiness =
        Readiness(
            consumers = listOf(consumerHealth),
            databaseOk = { true },
            deadLettersPublished = { 0 },
            deadLettersFailed = { 0 },
            positionsView = { capture.progress },
            limitsView = { null },
            reconciliation = { Reconciliation.of(emptyList(), System.currentTimeMillis()) },
        )
    private val server = DashboardServer(capture, broadcaster, WebAssets.load(), port = 0, readiness = readiness)
    private val client = HttpClient.newHttpClient()

    @BeforeAll
    fun start() {
        server.start()
    }

    @AfterAll
    fun stop() {
        server.stop()
        broadcaster.close()
    }

    private fun get(path: String): HttpResponse<String> =
        client.send(
            HttpRequest.newBuilder(URI("http://127.0.0.1:${server.boundPort}$path")).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    @Test
    fun `healthz answers ok — the process is up`() {
        val response = get("/healthz")
        assertEquals(200, response.statusCode())
        assertEquals("ok", response.body())
    }

    @Test
    fun `serves the privacy notice`() {
        val response = get("/privacy")
        assertEquals(200, response.statusCode())
        assertEquals("text/html; charset=utf-8", response.headers().firstValue("Content-Type").get())
        assertTrue(response.body().contains("Privacy"), response.body())
    }

    @Test
    fun `readyz answers 503 with the failing component named until the pipeline is healthy`() {
        // The consumer thread exists but has not polled with an assignment yet.
        val notReady = get("/readyz")
        assertEquals(503, notReady.statusCode())
        assertTrue(notReady.body().contains(""""ready":false"""), notReady.body())
        assertTrue(notReady.body().contains(""""test-consumer":{"ok":false"""), notReady.body())

        consumerHealth.assigned(1)
        consumerHealth.polled()
        val ready = get("/readyz")
        assertEquals(200, ready.statusCode())
        assertTrue(ready.body().contains(""""ready":true"""), ready.body())
        assertTrue(ready.body().contains(""""database":{"ok":true}"""), ready.body())
    }

    @Test
    fun `serves the UI with its content types`() {
        val index = get("/")
        assertEquals(200, index.statusCode())
        assertEquals("text/html; charset=utf-8", index.headers().firstValue("Content-Type").get())
        assertTrue(index.body().contains("TRADING SYSTEM"))

        assertEquals("text/css; charset=utf-8", get("/app.css").headers().firstValue("Content-Type").get())
        assertEquals("text/javascript; charset=utf-8", get("/app.js").headers().firstValue("Content-Type").get())
    }

    @Test
    fun `api state returns the current snapshot as JSON`() {
        val response = get("/api/state")
        assertEquals(200, response.statusCode())
        assertEquals("application/json", response.headers().firstValue("Content-Type").get())
        assertTrue(response.body().startsWith("""{"v":2,"positions":["""), response.body())
        assertTrue(response.body().contains(""""limits":{"""), response.body())
        assertTrue(response.body().contains(""""sync":{"""), response.body())
    }

    @Test
    fun `unknown paths are 404`() {
        assertEquals(404, get("/nope").statusCode())
    }

    @Test
    fun `non-GET methods are 405 with Allow`() {
        val response =
            client.send(
                HttpRequest
                    .newBuilder(URI("http://127.0.0.1:${server.boundPort}/api/state"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        assertEquals(405, response.statusCode())
        assertEquals("GET, HEAD", response.headers().firstValue("Allow").get())
    }

    @Test
    fun `HEAD answers every GET route with the GET's status and headers, minus the body`() {
        for (path in listOf("/", "/healthz", "/readyz", "/privacy", "/app.css", "/app.js", "/api/state")) {
            val head = head(path)
            assertEquals(get(path).statusCode(), head.statusCode(), path)
            assertEquals("", head.body(), path)
        }
        assertEquals("text/html; charset=utf-8", head("/").headers().firstValue("Content-Type").get())
    }

    @Test
    fun `HEAD on the stream answers headers without attaching to the broadcaster`() {
        val response = head("/api/stream")
        assertEquals(200, response.statusCode())
        assertEquals("text/event-stream", response.headers().firstValue("Content-Type").get())
        assertEquals("", response.body())
    }

    private fun head(path: String): HttpResponse<String> =
        client.send(
            HttpRequest
                .newBuilder(URI("http://127.0.0.1:${server.boundPort}$path"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    @Test
    fun `a server wired without a readiness probe reports plain readiness`() {
        val bare = DashboardServer(capture, broadcaster, WebAssets.load(), port = 0)
        bare.start()
        try {
            val response =
                client.send(
                    HttpRequest.newBuilder(URI("http://127.0.0.1:${bare.boundPort}/readyz")).GET().build(),
                    HttpResponse.BodyHandlers.ofString(),
                )
            assertEquals(200, response.statusCode())
            assertEquals("""{"ready":true}""", response.body())
        } finally {
            bare.stop()
        }
    }

    // Rejection happens before any handler runs, so the refused connection closes with no HTTP
    // status line — the client sees a connection-level failure, which is the documented contract.
    @Test
    fun `requests beyond the thread cap are refused rather than queued`() {
        val bounded = DashboardServer(capture, broadcaster, WebAssets.load(), port = 0, maxPoolThreads = 2)
        bounded.start()
        val streams = mutableListOf<HttpURLConnection>()
        try {
            repeat(2) {
                val connection =
                    URI("http://127.0.0.1:${bounded.boundPort}/api/stream").toURL().openConnection() as HttpURLConnection
                connection.readTimeout = 5_000
                val reader = connection.inputStream.bufferedReader()
                while (true) {
                    val line = reader.readLine() ?: error("stream closed before a data frame arrived")
                    if (line.startsWith("data: ")) break
                }
                streams.add(connection)
            }
            val request = HttpRequest.newBuilder(URI("http://127.0.0.1:${bounded.boundPort}/healthz")).GET().build()
            assertThrows(IOException::class.java) { client.send(request, HttpResponse.BodyHandlers.ofString()) }
        } finally {
            streams.forEach { it.disconnect() }
            bounded.stop()
        }
    }

    @Test
    fun `the SSE stream sends the current snapshot then pushes on each fill`() {
        val lines = mutableListOf<String>()
        val initial = CountDownLatch(1)
        val pushed = CountDownLatch(1)
        val reader =
            Thread {
                val request = HttpRequest.newBuilder(URI("http://127.0.0.1:${server.boundPort}/api/stream")).GET().build()
                client.send(request, HttpResponse.BodyHandlers.ofInputStream()).body().bufferedReader().use { body: BufferedReader ->
                    while (true) {
                        val line = body.readLine() ?: break
                        synchronized(lines) { lines.add(line) }
                        if (line.startsWith("data: ")) {
                            initial.countDown()
                            if (line.contains(""""quantity":3""")) pushed.countDown()
                        }
                    }
                }
            }.apply {
                isDaemon = true
                start()
            }

        assertTrue(initial.await(5, TimeUnit.SECONDS), "initial snapshot frame")
        capture.onFill(Fill("SIM", BigDecimal("100.00"), 3, 1, 2, Side.BID, 1000), FillSource("orderbook.fills", 0, 1))
        assertTrue(pushed.await(5, TimeUnit.SECONDS), "a fill pushes a fresh snapshot: ${synchronized(lines) { lines.toList() }}")
        reader.interrupt()
    }
}
