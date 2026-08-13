package com.damianhoward.tradingsystem.health

import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import java.lang.management.RuntimeMXBean
import java.lang.management.ThreadMXBean

/**
 * Process-level series, published alongside the readiness ones rather than instead of them.
 *
 * The distinction matters against risk-engine and trading-desk, which carry the same class for the
 * opposite reason: neither can render readiness at scrape frequency, so process metrics are all
 * their `/metrics` has. This service renders its whole readiness snapshot per scrape and is
 * document 17's reference for that rule. Nothing below restates a readiness condition; a gauge
 * that did would create a second number free to drift from the first.
 *
 * Heap against its ceiling is the load-bearing pair. Box 2 runs this beside a Kafka broker on 1 GB,
 * and this service's ceiling was set by guess at 128 MB with no measurement behind it at all — the
 * two services that were measured came in at roughly a tenth of theirs. One reading cannot size a
 * ceiling; a peak can, and a peak needs a series.
 */
class ProcessMetrics(
    private val runtime: RuntimeMXBean = ManagementFactory.getRuntimeMXBean(),
    private val memory: MemoryMXBean = ManagementFactory.getMemoryMXBean(),
    private val threads: ThreadMXBean = ManagementFactory.getThreadMXBean(),
    private val collectors: List<GarbageCollectorMXBean> = ManagementFactory.getGarbageCollectorMXBeans(),
) {
    fun render(): String {
        val out = StringBuilder()

        fun emit(
            name: String,
            help: String,
            type: String,
            samples: List<Pair<String, Any>>,
        ) {
            if (samples.isEmpty()) return
            out
                .append("# HELP ")
                .append(name)
                .append(' ')
                .append(help)
                .append('\n')
            out
                .append("# TYPE ")
                .append(name)
                .append(' ')
                .append(type)
                .append('\n')
            for ((labels, value) in samples) {
                out
                    .append(name)
                    .append(labels)
                    .append(' ')
                    .append(value)
                    .append('\n')
            }
        }

        fun gauge(
            name: String,
            help: String,
            value: Any,
        ) = emit(name, help, "gauge", listOf("" to value))

        gauge(
            "${PREFIX}process_uptime_seconds",
            "Seconds since this process started. A reset is a restart, wanted or not.",
            seconds(runtime.uptime),
        )

        val heap = memory.heapMemoryUsage
        gauge("${PREFIX}jvm_heap_used_bytes", "Heap in use after the last collection this reading saw.", heap.used)
        gauge("${PREFIX}jvm_heap_committed_bytes", "Heap the JVM currently holds from the operating system.", heap.committed)
        // -1 means no ceiling was configured. Publishing it would read as a real limit of minus one
        // byte, and every "used against max" expression built on it would be nonsense.
        if (heap.max >= 0) {
            gauge(
                "${PREFIX}jvm_heap_max_bytes",
                "The configured heap ceiling. Used against this is what the ceilings were guessed at.",
                heap.max,
            )
        }

        gauge("${PREFIX}jvm_threads", "Live threads, daemon and non-daemon.", threads.threadCount)

        // The collector names come from the JVM's own configuration, so the label set is fixed at
        // startup — bounded by configuration rather than by data, which is the rule that keeps a
        // series from being minted per observation.
        emit(
            "${PREFIX}jvm_gc_collections_total",
            "Collections each garbage collector has completed.",
            "counter",
            collectors.map { """{gc=${quote(it.name)}}""" to it.collectionCount },
        )
        emit(
            "${PREFIX}jvm_gc_seconds_total",
            "Seconds each garbage collector has spent collecting.",
            "counter",
            collectors.map { """{gc=${quote(it.name)}}""" to seconds(it.collectionTime) },
        )

        return out.toString()
    }

    // Rendered rather than divided into a Double, so a duration never reaches the endpoint in
    // exponential notation — Prometheus accepts it, humans reading a curl do not.
    private fun seconds(millis: Long): String = "%d.%03d".format(millis / 1000, millis % 1000)

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

    companion object {
        private const val PREFIX = "trading_system_"

        /**
         * The Prometheus exposition content type. The version parameter is not decoration: a
         * collector reads it to decide which parser to use.
         */
        const val CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
    }
}
