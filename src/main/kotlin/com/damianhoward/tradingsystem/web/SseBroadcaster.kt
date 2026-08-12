package com.damianhoward.tradingsystem.web

import com.sun.net.httpserver.HttpExchange
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/** Pushes live state to browsers. SSE today ([SseBroadcaster]); a WebSocket impl would satisfy it unchanged. */
interface Broadcaster {
    fun startHeartbeat(periodSeconds: Long = 15)

    fun broadcast(json: String)

    fun stream(
        exchange: HttpExchange,
        initialJson: String,
    )
}

/**
 * SSE fan-out: each client drains its own bounded queue so a slow one can't block the others or
 * grow the heap; a heartbeat keeps connections alive through proxies and reaps any client whose
 * write has started failing.
 */
class SseBroadcaster(
    private val queueCapacity: Int = 64,
) : Broadcaster,
    AutoCloseable {
    private class Client(
        capacity: Int,
    ) {
        val queue = ArrayBlockingQueue<String>(capacity)
    }

    private val clients = CopyOnWriteArrayList<Client>()
    private val heartbeat = Executors.newSingleThreadScheduledExecutor { Thread(it, "sse-heartbeat").apply { isDaemon = true } }

    override fun startHeartbeat(periodSeconds: Long) {
        heartbeat.scheduleAtFixedRate({ enqueue(": ping\n\n") }, periodSeconds, periodSeconds, TimeUnit.SECONDS)
    }

    override fun broadcast(json: String) = enqueue(asEvent(json))

    override fun stream(
        exchange: HttpExchange,
        initialJson: String,
    ) {
        exchange.responseHeaders.add("Content-Type", "text/event-stream; charset=utf-8")
        exchange.responseHeaders.add("Cache-Control", "no-cache")
        exchange.responseHeaders.add("Connection", "keep-alive")
        exchange.sendResponseHeaders(200, 0)
        val out = exchange.responseBody
        val client = Client(queueCapacity)
        try {
            // Register before the initial write: a snapshot broadcast during the write would
            // otherwise be missed entirely, leaving this client stale until the next fill.
            // Queued frames are complete snapshots newer than the initial one, so draining them
            // straight after it keeps the stream monotonic.
            clients.add(client)
            write(out, "retry: 3000\n\n" + asEvent(initialJson))
            while (true) {
                write(out, client.queue.take())
            }
        } catch (_: Exception) {
            // Client gone or server shutting down — fall through to cleanup.
        } finally {
            clients.remove(client)
            runCatching { exchange.close() }
        }
    }

    override fun close() {
        heartbeat.shutdownNow()
    }

    /** Currently-registered clients — lets tests await registration instead of sleeping. */
    internal fun clientCount(): Int = clients.size

    // Backpressure by dropping the *oldest* frame when a client's queue is full: every frame is a
    // complete snapshot, so a stalled client that recovers renders the latest state, not a backlog.
    private fun enqueue(frame: String) =
        clients.forEach { client ->
            while (!client.queue.offer(frame)) {
                client.queue.poll()
            }
        }

    private fun asEvent(json: String) = "data: $json\n\n"

    private fun write(
        out: OutputStream,
        frame: String,
    ) {
        out.write(frame.toByteArray(StandardCharsets.UTF_8))
        out.flush()
    }
}
