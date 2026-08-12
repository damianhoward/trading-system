package com.damianhoward.tradingsystem.web

import com.sun.net.httpserver.Headers
import com.sun.net.httpserver.HttpContext
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpPrincipal
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** Drives [SseBroadcaster.stream] directly with a scripted exchange to exercise the fan-out edge cases. */
class SseBroadcasterTest {
    @Test
    fun `a client receives the initial snapshot then broadcast frames`() {
        val broadcaster = SseBroadcaster()
        val exchange = FakeExchange(ByteArrayOutputStream())
        val streaming = streamOnThread(broadcaster, exchange, initialJson = """{"seq":0}""")

        awaitOutput(exchange) { it.contains("""data: {"seq":0}""") }
        awaitRegistered(broadcaster)
        broadcaster.broadcast("""{"seq":1}""")
        awaitOutput(exchange) { it.contains("""data: {"seq":1}""") }

        assertEquals(200, exchange.responseCode)
        assertEquals("text/event-stream; charset=utf-8", exchange.responseHeaders.getFirst("Content-Type"))
        streaming.interrupt()
        streaming.join(2_000)
        assertFalse(streaming.isAlive, "stream should exit once the take() is interrupted")
        broadcaster.close()
    }

    @Test
    fun `the heartbeat keeps a connected client fed`() {
        val broadcaster = SseBroadcaster()
        val exchange = FakeExchange(ByteArrayOutputStream())
        val streaming = streamOnThread(broadcaster, exchange, initialJson = "{}")

        broadcaster.startHeartbeat(periodSeconds = 1)
        awaitOutput(exchange, timeoutMillis = 5_000) { it.contains(": ping") }

        streaming.interrupt()
        streaming.join(2_000)
        broadcaster.close()
    }

    @Test
    fun `a client whose writes fail is reaped and its exchange closed`() {
        val broadcaster = SseBroadcaster()
        val exchange = FakeExchange(FailAfterFirstWrite())
        val streaming = streamOnThread(broadcaster, exchange, initialJson = "{}")

        awaitRegistered(broadcaster)
        broadcaster.broadcast("boom")
        streaming.join(2_000)

        assertFalse(streaming.isAlive, "a failing writer should end the stream")
        assertTrue(exchange.closed, "the dead client's exchange should be closed")
        broadcaster.close()
    }

    @Test
    fun `a stalled client drops the oldest frames, not the newest`() {
        val broadcaster = SseBroadcaster(queueCapacity = 2)
        val gate = CountDownLatch(1)
        val output = BlockingAfterFirstWrite(gate)
        val exchange = FakeExchange(output)
        val streaming = streamOnThread(broadcaster, exchange, initialJson = "init")

        awaitOutput(exchange) { it.contains("data: init") }
        awaitRegistered(broadcaster) // next sink write will block on the gate
        broadcaster.broadcast("stale-1") // taken by the writer, blocks in write()
        (2..5).forEach { broadcaster.broadcast("frame-$it") } // capacity 2: only frame-4/frame-5 survive
        gate.countDown()

        awaitOutput(exchange) { it.contains("data: frame-5") }
        val written = exchange.written()
        assertFalse(written.contains("data: frame-2"), "oldest overflow frame should have been dropped")
        assertFalse(written.contains("data: frame-3"), "oldest overflow frame should have been dropped")
        assertTrue(written.contains("data: frame-4"))

        streaming.interrupt()
        streaming.join(2_000)
        broadcaster.close()
    }

    private fun streamOnThread(
        broadcaster: SseBroadcaster,
        exchange: FakeExchange,
        initialJson: String,
    ): Thread =
        Thread { broadcaster.stream(exchange, initialJson) }.apply {
            isDaemon = true
            start()
        }

    private fun awaitRegistered(broadcaster: SseBroadcaster) {
        val deadline = System.currentTimeMillis() + 2_000
        while (broadcaster.clientCount() == 0) {
            check(System.currentTimeMillis() < deadline) { "client never registered" }
            Thread.sleep(5)
        }
    }

    private fun awaitOutput(
        exchange: FakeExchange,
        timeoutMillis: Long = 2_000,
        predicate: (String) -> Boolean,
    ) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            if (predicate(exchange.written())) return
            Thread.sleep(10)
        }
        throw AssertionError("expected output not observed within ${timeoutMillis}ms; got: ${exchange.written()}")
    }

    /** Collects writes; wraps any [OutputStream] so tests can script failures and stalls. */
    private class FakeExchange(
        private val sink: OutputStream,
    ) : HttpExchange() {
        private val headers = Headers()
        private var status = 0

        @Volatile
        var closed = false

        private val capture = ByteArrayOutputStream()

        fun written(): String = synchronized(capture) { capture.toString(StandardCharsets.UTF_8) }

        private val tee =
            object : OutputStream() {
                override fun write(b: Int) {
                    sink.write(b)
                    synchronized(capture) { capture.write(b) }
                }

                override fun write(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ) {
                    sink.write(b, off, len)
                    synchronized(capture) { capture.write(b, off, len) }
                }
            }

        override fun getRequestHeaders(): Headers = Headers()

        override fun getResponseHeaders(): Headers = headers

        override fun getRequestURI(): URI = URI("/api/stream")

        override fun getRequestMethod(): String = "GET"

        override fun getHttpContext(): HttpContext? = null

        override fun close() {
            closed = true
        }

        override fun getRequestBody(): InputStream = InputStream.nullInputStream()

        override fun getResponseBody(): OutputStream = tee

        override fun sendResponseHeaders(
            rCode: Int,
            responseLength: Long,
        ) {
            status = rCode
        }

        override fun getRemoteAddress(): InetSocketAddress = InetSocketAddress(0)

        override fun getResponseCode(): Int = status

        override fun getLocalAddress(): InetSocketAddress = InetSocketAddress(0)

        override fun getProtocol(): String = "HTTP/1.1"

        override fun getAttribute(name: String?): Any? = null

        override fun setAttribute(
            name: String?,
            value: Any?,
        ) {
        }

        override fun setStreams(
            i: InputStream?,
            o: OutputStream?,
        ) {
        }

        override fun getPrincipal(): HttpPrincipal? = null
    }

    private class FailAfterFirstWrite : OutputStream() {
        private var writes = 0

        override fun write(b: Int): Unit = throw IOException("scripted failure")

        override fun write(
            b: ByteArray,
            off: Int,
            len: Int,
        ) {
            if (writes++ > 0) throw IOException("scripted failure")
        }
    }

    private class BlockingAfterFirstWrite(
        private val gate: CountDownLatch,
    ) : OutputStream() {
        private var writes = 0

        override fun write(b: Int) {}

        override fun write(
            b: ByteArray,
            off: Int,
            len: Int,
        ) {
            if (writes++ > 0) {
                check(gate.await(10, TimeUnit.SECONDS)) { "test gate never opened" }
            }
        }
    }
}
