package com.damianhoward.tradingsystem.health

import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryType
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
 * ceiling; a peak can, and this used to say a peak needs a series. It does not — the JVM keeps the
 * high-water mark itself, which is what [heapPeak] reads.
 *
 * That matters most here. Box 2 is the box that swaps, and this is the service a rebalance would
 * move off it, so its real requirement is the number that decides whether box 1 — which has no swap
 * and therefore no slowdown mode, only the OOM killer — can hold it. A sampled maximum would
 * under-read that requirement by however much the five-minute scrape missed, and the sampling error
 * runs in the direction that makes the move look safer than it is. [heapPostGc] is the other half:
 * peak is what this process touched, the live set is what it needs, and only the second belongs in
 * that arithmetic.
 */
class ProcessMetrics(
    private val runtime: RuntimeMXBean = ManagementFactory.getRuntimeMXBean(),
    private val memory: MemoryMXBean = ManagementFactory.getMemoryMXBean(),
    private val threads: ThreadMXBean = ManagementFactory.getThreadMXBean(),
    private val collectors: List<GarbageCollectorMXBean> = ManagementFactory.getGarbageCollectorMXBeans(),
    pools: List<MemoryPoolMXBean> = ManagementFactory.getMemoryPoolMXBeans(),
) {
    // Heap pools only. Metaspace and the code cache are memory this process occupies and neither is
    // governed by the heap ceiling, so summing them here would inflate a number whose whole purpose
    // is to be compared against -Xmx.
    private val heapPools = pools.filter { it.type == MemoryType.HEAP }

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

        // Summed across heap pools, and the sum is an over-estimate: pools reach their own peaks at
        // different moments, so adding those peaks can exceed any total that was ever simultaneously
        // live. That error has a direction, and it is the safe one for sizing a ceiling — it cannot
        // talk a ceiling down below what the process needed. Read it as an upper bound rather than a
        // measurement, which is what the pair with the post-collection gauge below is for.
        //
        // Monotonic since start, so a restart resets it. That is the same reset uptime already
        // reports and the reason it is published beside one.
        heapPeak()?.let {
            gauge(
                "${PREFIX}jvm_heap_peak_bytes",
                "Highest heap the JVM has held since start, summed across heap pools. An upper bound, not a reading.",
                it,
            )
        }

        // Usage after the most recent collection: the live set, which is what a ceiling has to hold.
        // A pool answers null until it has been collected at all, so on a young process this is
        // absent rather than zero — and absent is the honest answer, since nothing has established
        // a live set yet. Zero would read as an empty heap, which is a different claim.
        heapPostGc()?.let {
            gauge(
                "${PREFIX}jvm_heap_post_gc_bytes",
                "Heap still live after the last collection, summed across heap pools. The number a ceiling must cover.",
                it,
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

    // Null rather than zero when there are no heap pools to ask. A JVM always has some, so this is
    // the substituted-bean case in a test rather than anything a live process does — but a summed
    // zero would publish "this heap has never held anything", which is a claim about the process
    // instead of about the absence of an answer.
    private fun heapPeak(): Long? = heapPools.ifEmpty { null }?.sumOf { it.peakUsage.used }

    // Null until something has been collected. getCollectionUsage is null on a pool the collector
    // has not touched, and unsupported on some pools outright, so a partial sum would silently
    // report a live set missing whichever pools stayed quiet.
    private fun heapPostGc(): Long? {
        val usages = heapPools.map { it.collectionUsage }
        return if (usages.isEmpty() || usages.any { it == null }) null else usages.sumOf { it!!.used }
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
