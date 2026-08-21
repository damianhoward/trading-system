package com.damianhoward.tradingsystem.health

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.lang.management.ManagementFactory
import java.lang.management.MemoryPoolMXBean
import java.lang.management.MemoryType
import java.lang.management.MemoryUsage
import javax.management.ObjectName

/**
 * The two heap gauges exist to answer "what ceiling does this service need" without a fast series,
 * so what is worth asserting is the shape of the answer when the JVM cannot give one — a heap that
 * has never been collected, and a pool that does not report collection usage at all. Both must be
 * absent rather than zero, because zero is a claim about the heap and absence is a claim about the
 * measurement.
 */
class ProcessMetricsTest {
    /** Only the four methods under test are real; the rest of the interface is never reached. */
    private class Pool(
        private val poolType: MemoryType,
        private val peak: Long,
        private val collection: MemoryUsage?,
    ) : MemoryPoolMXBean {
        override fun getType() = poolType

        override fun getPeakUsage() = MemoryUsage(0, peak, peak, peak)

        override fun getCollectionUsage() = collection

        override fun getName() = "stub"

        override fun getUsage() = MemoryUsage(0, 1, 1, 1)

        override fun isValid() = true

        override fun getMemoryManagerNames() = arrayOf("stub")

        override fun getUsageThreshold() = 0L

        override fun setUsageThreshold(threshold: Long) = Unit

        override fun isUsageThresholdExceeded() = false

        override fun getUsageThresholdCount() = 0L

        override fun isUsageThresholdSupported() = false

        override fun getCollectionUsageThreshold() = 0L

        override fun setCollectionUsageThreshold(threshold: Long) = Unit

        override fun isCollectionUsageThresholdExceeded() = false

        override fun getCollectionUsageThresholdCount() = 0L

        override fun isCollectionUsageThresholdSupported() = false

        override fun resetPeakUsage() = Unit

        override fun getObjectName(): ObjectName = ObjectName("java.lang:type=MemoryPool,name=stub")
    }

    private fun metricsWith(pools: List<MemoryPoolMXBean>) =
        ProcessMetrics(
            runtime = ManagementFactory.getRuntimeMXBean(),
            memory = ManagementFactory.getMemoryMXBean(),
            threads = ManagementFactory.getThreadMXBean(),
            collectors = ManagementFactory.getGarbageCollectorMXBeans(),
            pools = pools,
        ).render()

    private fun usage(used: Long) = MemoryUsage(0, used, used, used)

    @Test
    fun `peak and post-collection are summed across heap pools`() {
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.HEAP, peak = 100, collection = usage(30)),
                    Pool(MemoryType.HEAP, peak = 50, collection = usage(20)),
                ),
            )
        assertTrue(metrics.contains("trading_system_jvm_heap_peak_bytes 150"), metrics)
        assertTrue(metrics.contains("trading_system_jvm_heap_post_gc_bytes 50"), metrics)
    }

    @Test
    fun `non-heap pools are excluded from both`() {
        // Metaspace is not governed by the heap ceiling, so counting it would inflate the number
        // whose only purpose is comparison against -Xmx.
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.HEAP, peak = 100, collection = usage(30)),
                    Pool(MemoryType.NON_HEAP, peak = 900, collection = usage(800)),
                ),
            )
        assertTrue(metrics.contains("trading_system_jvm_heap_peak_bytes 100"), metrics)
        assertTrue(metrics.contains("trading_system_jvm_heap_post_gc_bytes 30"), metrics)
    }

    @Test
    fun `a heap not yet collected publishes no live set at all`() {
        val metrics = metricsWith(listOf(Pool(MemoryType.HEAP, peak = 100, collection = null)))
        assertTrue(metrics.contains("trading_system_jvm_heap_peak_bytes 100"), metrics)
        // Zero would read as an empty heap; the fact is that nothing has established a live set.
        assertFalse(metrics.contains("trading_system_jvm_heap_post_gc_bytes"), metrics)
    }

    @Test
    fun `one silent pool withdraws the live set rather than under-reporting it`() {
        // A partial sum would look like a smaller live set, which is the direction that talks a
        // ceiling down — the error this pair of gauges exists to prevent.
        val metrics =
            metricsWith(
                listOf(
                    Pool(MemoryType.HEAP, peak = 100, collection = usage(30)),
                    Pool(MemoryType.HEAP, peak = 50, collection = null),
                ),
            )
        assertTrue(metrics.contains("trading_system_jvm_heap_peak_bytes 150"), metrics)
        assertFalse(metrics.contains("trading_system_jvm_heap_post_gc_bytes"), metrics)
    }

    @Test
    fun `no heap pools publishes neither gauge`() {
        val metrics = metricsWith(listOf(Pool(MemoryType.NON_HEAP, peak = 900, collection = usage(800))))
        assertFalse(metrics.contains("trading_system_jvm_heap_peak_bytes"), metrics)
        assertFalse(metrics.contains("trading_system_jvm_heap_post_gc_bytes"), metrics)
    }

    @Test
    fun `the real JVM answers both, and the peak is not below the live set`() {
        // Against the actual MXBeans rather than stubs, so the gauges are known to work on the JVM
        // the service runs on and not only against the shapes this test imagines.
        System.gc()
        val metrics = ProcessMetrics().render()
        val peak = valueOf(metrics, "trading_system_jvm_heap_peak_bytes")
        val live = valueOf(metrics, "trading_system_jvm_heap_post_gc_bytes")
        assertTrue(peak != null && peak > 0, metrics)
        assertTrue(live != null && live > 0, metrics)
        assertTrue(peak!! >= live!!, "peak $peak should not be below live set $live")
    }

    @Test
    fun `no series is declared twice`() {
        // The pair added here is the first chance to publish the same name from two places, and a
        // duplicate is what a collector reads as a series disagreeing with itself.
        val names =
            ProcessMetrics()
                .render()
                .lineSequence()
                .filter { it.startsWith("# TYPE ") }
                .map { it.split(' ')[2] }
                .toList()
        assertEquals(names.size, names.toSet().size, names.toString())
    }

    private fun valueOf(
        metrics: String,
        name: String,
    ): Long? =
        metrics
            .lineSequence()
            .firstOrNull { it.startsWith("$name ") }
            ?.substringAfter(' ')
            ?.trim()
            ?.toLong()
}
