package com.damianhoward.tradingsystem.consume

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets

class DltReplayTest {
    private val dltTopic = "orderbook.fills.DLT"
    private val fillsTopic = "orderbook.fills"
    private val partition = TopicPartition(dltTopic, 0)

    private val goodJson =
        """{"v":1,"symbol":"SIM","price":"101.00000000","size":5,""" +
            """"makerOrderId":12,"takerOrderId":34,"aggressor":"BID","ts":1000}"""

    private fun consumer(): MockConsumer<String, String> =
        MockConsumer<String, String>("earliest").apply {
            updatePartitions(dltTopic, listOf(PartitionInfo(dltTopic, 0, null, null, null)))
            updateBeginningOffsets(mapOf(partition to 0L))
        }

    private fun producer() = MockProducer(true, null, StringSerializer(), StringSerializer())

    private fun record(
        offset: Long,
        value: String,
    ): ConsumerRecord<String, String> =
        ConsumerRecord(dltTopic, 0, offset, "SIM", value).apply {
            headers().add("dlt.source.offset", offset.toString().toByteArray(StandardCharsets.UTF_8))
        }

    @Test
    fun `replays records that now parse, leaves still-malformed ones, ignores records past the end snapshot`() {
        val consumer = consumer()
        consumer.updateEndOffsets(mapOf(partition to 3L))
        val producer = producer()
        val logged = mutableListOf<String>()
        consumer.schedulePollTask {
            consumer.addRecord(record(0, goodJson))
            consumer.addRecord(record(1, "not json"))
            consumer.addRecord(record(2, goodJson))
            // Dead-lettered after the run attached: waits for the next run.
            consumer.addRecord(record(3, goodJson))
        }

        val summary = DltReplay(consumer, producer, dltTopic, fillsTopic, log = logged::add).run()

        assertEquals(DltReplay.Summary(replayed = 2, stillMalformed = 1), summary)
        assertEquals(2, producer.history().size, "only in-bounds, now-parseable records re-enter the stream")
        producer.history().forEach { sent ->
            assertEquals(fillsTopic, sent.topic())
            assertEquals("SIM", sent.key(), "the original key keeps per-symbol ordering on replay")
            assertEquals(goodJson, sent.value())
        }
        assertEquals(
            listOf("0", "2"),
            producer.history().map { String(it.headers().lastHeader("dlt.source.offset").value(), StandardCharsets.UTF_8) },
            "provenance headers travel with the replayed copy",
        )
        assertTrue(logged.any { it.startsWith("replayed $dltTopic-0@0") }, "each replay is logged for the audit trail")
        assertTrue(logged.any { it.startsWith("left still-malformed $dltTopic-0@1") })
    }

    @Test
    fun `an empty dead-letter topic replays nothing`() {
        val consumer = consumer()
        consumer.updateEndOffsets(mapOf(partition to 0L))
        val producer = producer()

        val summary = DltReplay(consumer, producer, dltTopic, fillsTopic).run()

        assertEquals(DltReplay.Summary(replayed = 0, stillMalformed = 0), summary)
        assertTrue(producer.history().isEmpty())
    }

    @Test
    fun `a topic that does not exist yet replays nothing`() {
        val consumer = MockConsumer<String, String>("earliest")
        val producer = producer()

        val summary = DltReplay(consumer, producer, dltTopic, fillsTopic).run()

        assertEquals(DltReplay.Summary(replayed = 0, stillMalformed = 0), summary)
        assertTrue(producer.history().isEmpty())
    }

    @Test
    fun `close closes both clients`() {
        val consumer = consumer()
        val producer = producer()

        DltReplay(consumer, producer, dltTopic, fillsTopic).close()

        assertTrue(consumer.closed())
        assertTrue(producer.closed())
    }
}
