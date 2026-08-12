package com.damianhoward.tradingsystem.web

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import com.damianhoward.tradingsystem.capture.TradeCapture
import com.damianhoward.tradingsystem.health.Readiness
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.ExecutorService
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

/**
 * HTTP transport for the dashboard: the static UI, the current state as JSON, and an SSE stream
 * that pushes a fresh snapshot on every fill. Plumbing only — every number comes from
 * [TradeCapture]'s snapshot over risk-engine's calculators, and the front end is a thin renderer
 * of [com.damianhoward.tradingsystem.view.DashboardSnapshot.toJson]. JDK [HttpServer] on a
 * request pool capped at [maxPoolThreads], no web framework; each SSE stream pins one pool thread
 * for its connection's lifetime, and requests beyond the cap are refused at the connection rather
 * than queued.
 */
class DashboardServer(
    private val capture: TradeCapture,
    private val broadcaster: Broadcaster,
    private val assets: WebAssets,
    private val port: Int,
    private val readiness: Readiness? = null,
    private val maxPoolThreads: Int = 64,
    /**
     * Loopback by default: this server is meant to be reached through the reverse proxy that
     * terminates TLS, applies the security headers and writes the access log, never directly.
     * Binding every interface — which `InetSocketAddress(port)` alone does — makes that
     * guarantee depend on a firewall rule being right somewhere else. Appended last so existing
     * positional call sites are unaffected.
     */
    private val bindAddress: InetAddress = InetAddress.getLoopbackAddress(),
) {
    private lateinit var server: HttpServer
    private lateinit var executor: ExecutorService

    /** Binds and starts serving; requesting port 0 binds an ephemeral port (see [boundPort]). */
    fun start() {
        server = HttpServer.create(InetSocketAddress(bindAddress, port), 0)
        // Cached-pool reuse and keep-alive but with a hard thread ceiling: SSE streams hold their
        // pool thread, so an unbounded pool lets slow-reading clients grow memory without limit.
        // No work queue — a request queued behind saturated SSE streams would wait forever, so
        // saturation refuses the new connection instead.
        executor =
            ThreadPoolExecutor(0, maxPoolThreads, 60L, TimeUnit.SECONDS, SynchronousQueue()) {
                Thread(it).apply { isDaemon = true }
            }
        server.executor = executor
        server.createContext("/", ::route)
        server.start()
        println("Trading system dashboard listening on :$boundPort")
    }

    /** The port actually bound — differs from the requested one when 0 (ephemeral) was asked for. */
    val boundPort: Int get() = server.address.port

    /** Stops accepting connections and shuts down the request pool this server created. */
    fun stop() {
        server.stop(0)
        executor.shutdownNow()
    }

    private fun route(exchange: HttpExchange) {
        when (exchange.requestURI.path) {
            "/healthz" -> get(exchange) { respond(exchange, 200, "text/plain", "ok") }
            "/readyz" -> get(exchange) { ready(exchange) }
            "/" -> get(exchange) { respond(exchange, 200, "text/html; charset=utf-8", assets.indexHtml) }
            "/privacy" -> get(exchange) { respond(exchange, 200, "text/html; charset=utf-8", assets.privacyHtml) }
            "/app.css" -> get(exchange) { respond(exchange, 200, "text/css; charset=utf-8", assets.appCss) }
            "/app.js" -> get(exchange) { respond(exchange, 200, "text/javascript; charset=utf-8", assets.appJs) }
            "/api/state" -> get(exchange) { respond(exchange, 200, "application/json", capture.snapshot().toJson()) }
            "/api/stream" ->
                get(exchange) {
                    // A HEAD must not attach to the broadcaster — it wants headers, not a stream.
                    if (exchange.requestMethod == "HEAD") {
                        respond(exchange, 200, "text/event-stream", "")
                    } else {
                        broadcaster.stream(exchange, capture.snapshot().toJson())
                    }
                }
            else -> respond(exchange, 404, "text/plain", "not found")
        }
    }

    /** Liveness says the process answers; this says the pipeline works — 503 when it does not. */
    private fun ready(exchange: HttpExchange) {
        val probe = readiness?.probe()
        if (probe == null) {
            respond(exchange, 200, "application/json", """{"ready":true}""")
        } else {
            respond(exchange, if (probe.ready) 200 else 503, "application/json", probe.json)
        }
    }

    // HEAD rides every GET route: the handler runs identically and respond() suppresses the body,
    // so the status and headers a HEAD probe sees are the ones the GET would have produced.
    private inline fun get(
        exchange: HttpExchange,
        handler: () -> Unit,
    ) {
        if (exchange.requestMethod == "GET" || exchange.requestMethod == "HEAD") {
            handler()
        } else {
            exchange.responseHeaders.add("Allow", "GET, HEAD")
            respond(exchange, 405, "text/plain", "method not allowed")
        }
    }

    private fun respond(
        exchange: HttpExchange,
        status: Int,
        contentType: String,
        body: String,
    ) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.responseHeaders.add("Content-Type", contentType)
        if (exchange.requestMethod == "HEAD") {
            // Headers only: -1 tells the JDK server no body follows, which is the one length it
            // accepts on a HEAD without logging a warning (it drops Content-Length either way).
            exchange.sendResponseHeaders(status, -1)
            exchange.close()
        } else {
            exchange.sendResponseHeaders(status, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
    }
}
