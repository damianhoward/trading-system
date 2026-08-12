package com.damianhoward.tradingsystem.consume

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * The poll loop's own semantics over a [MockConsumer]: health reporting, the fatal path, and
 * ledger-seek attachment. The real-broker behaviour (metadata, wakeup, fan-out) is covered by
 * [FillPipelineIntegrationTest].
 */
class FillConsumerTest {
    private val topic = "orderbook.fills"
    private val partition = TopicPartition(topic, 0)

    private fun mockConsumer(): MockConsumer<String, String> = MockConsumer("earliest")

    private fun record(offset: Long): ConsumerRecord<String, String> = ConsumerRecord(topic, 0, offset, "SIM", "payload-$offset")

    @Test
    fun `an unexpected poll failure marks health fatal and calls onFatal — never a silent death`() {
        val consumer = mockConsumer()
        consumer.updatePartitions(topic, listOf(PartitionInfo(topic, 0, null, null, null)))
        consumer.updateBeginningOffsets(mapOf(partition to 0L))
        val fatal = CountDownLatch(1)
        var seen: Exception? = null
        val fillConsumer =
            FillConsumer(
                consumer = consumer,
                topic = topic,
                handler = RecordHandler {},
                health = ConsumerHealth("fills"),
                startOffsets = emptyMap(),
                pollTimeout = Duration.ofMillis(10),
                onFatal = { error ->
                    seen = error
                    fatal.countDown()
                },
            )
        consumer.schedulePollTask { throw IllegalStateException("broker said no") }

        fillConsumer.start()
        try {
            assertTrue(fatal.await(5, TimeUnit.SECONDS), "onFatal must fire")
            assertEquals("broker said no", seen?.message)
            assertFalse(fillConsumer.health.threadAlive)
            assertNotNull(fillConsumer.health.fatal)
            assertTrue(fillConsumer.health.fatal!!.contains("broker said no"))
        } finally {
            fillConsumer.close()
        }
    }

    @Test
    fun `seek mode starts each partition just past its recorded offset`() {
        val consumer = mockConsumer()
        consumer.updatePartitions(topic, listOf(PartitionInfo(topic, 0, null, null, null)))
        consumer.updateBeginningOffsets(mapOf(partition to 0L))
        val handled = ConcurrentLinkedQueue<Long>()
        val caughtUp = CountDownLatch(1)
        val fillConsumer =
            FillConsumer(
                consumer = consumer,
                topic = topic,
                handler =
                    RecordHandler { record ->
                        handled.add(record.offset())
                        if (record.offset() == 43L) caughtUp.countDown()
                    },
                health = ConsumerHealth("limits"),
                startOffsets = mapOf(0 to 41L),
                pollTimeout = Duration.ofMillis(10),
            )
        consumer.schedulePollTask {
            consumer.addRecord(record(42))
            consumer.addRecord(record(43))
        }

        fillConsumer.start()
        try {
            assertTrue(caughtUp.await(5, TimeUnit.SECONDS), "records after the ledger mark must arrive")
            assertEquals(listOf(42L, 43L), handled.toList(), "the ledger already holds offsets up to 41")
            assertTrue(fillConsumer.health.assigned)
        } finally {
            fillConsumer.close()
        }
    }

    @Test
    fun `seek mode without a recorded offset reads the partition from the beginning`() {
        val consumer = mockConsumer()
        consumer.updatePartitions(topic, listOf(PartitionInfo(topic, 0, null, null, null)))
        consumer.updateBeginningOffsets(mapOf(partition to 0L))
        val first = CountDownLatch(1)
        val handled = ConcurrentLinkedQueue<Long>()
        val fillConsumer =
            FillConsumer(
                consumer = consumer,
                topic = topic,
                handler =
                    RecordHandler { record ->
                        handled.add(record.offset())
                        first.countDown()
                    },
                health = ConsumerHealth("limits"),
                startOffsets = emptyMap(),
                pollTimeout = Duration.ofMillis(10),
            )
        consumer.schedulePollTask { consumer.addRecord(record(0)) }

        fillConsumer.start()
        try {
            assertTrue(first.await(5, TimeUnit.SECONDS))
            assertEquals(listOf(0L), handled.toList())
        } finally {
            fillConsumer.close()
        }
    }

    @Test
    fun `a clean close stops the thread and leaves no fatal state`() {
        val consumer = mockConsumer()
        consumer.updatePartitions(topic, listOf(PartitionInfo(topic, 0, null, null, null)))
        consumer.updateBeginningOffsets(mapOf(partition to 0L))
        val fillConsumer =
            FillConsumer(
                consumer = consumer,
                topic = topic,
                handler = RecordHandler {},
                health = ConsumerHealth("fills"),
                startOffsets = emptyMap(),
                pollTimeout = Duration.ofMillis(10),
            )

        fillConsumer.start()
        fillConsumer.close()

        assertFalse(fillConsumer.health.threadAlive)
        assertEquals(null, fillConsumer.health.fatal)
        assertTrue(consumer.closed())
    }
}
