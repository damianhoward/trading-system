package com.damianhoward.tradingsystem.consume

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets
import java.time.Duration

class DeadLetterPublisherTest {
    private fun record(value: String = """{"bad":true}""") = ConsumerRecord("orderbook.fills", 0, 42L, "SIM", value)

    @Test
    fun `publishes the original record with error and source headers, confirmed by the broker`() {
        val producer = MockProducer(true, null, StringSerializer(), StringSerializer())
        val publisher = DeadLetterPublisher(producer, "orderbook.fills.DLT")

        publisher.publish(record(), IllegalArgumentException("missing field: price"))

        val sent = producer.history().single()
        assertEquals("orderbook.fills.DLT", sent.topic())
        assertEquals("SIM", sent.key())
        assertEquals("""{"bad":true}""", sent.value())
        assertEquals("java.lang.IllegalArgumentException", header(sent.headers().lastHeader("dlt.error.class")))
        assertEquals("missing field: price", header(sent.headers().lastHeader("dlt.error.message")))
        assertEquals("orderbook.fills", header(sent.headers().lastHeader("dlt.source.topic")))
        assertEquals("0", header(sent.headers().lastHeader("dlt.source.partition")))
        assertEquals("42", header(sent.headers().lastHeader("dlt.source.offset")))
        assertEquals(1L, publisher.published)
        assertEquals(0L, publisher.failed)
    }

    @Test
    fun `an exception with no message writes an empty header`() {
        val producer = MockProducer(true, null, StringSerializer(), StringSerializer())
        val publisher = DeadLetterPublisher(producer)

        publisher.publish(record(), IllegalStateException())

        assertEquals(
            "",
            header(
                producer
                    .history()
                    .single()
                    .headers()
                    .lastHeader("dlt.error.message"),
            ),
        )
    }

    @Test
    fun `an unacknowledged send throws and is counted — the caller must not commit past it`() {
        // Auto-complete off: the broker never acks within the confirm window.
        val producer = MockProducer(false, null, StringSerializer(), StringSerializer())
        val publisher = DeadLetterPublisher(producer, confirmTimeout = Duration.ofMillis(50))

        assertThrows(DeadLetterPublishException::class.java) {
            publisher.publish(record(), IllegalArgumentException("boom"))
        }
        assertEquals(0L, publisher.published)
        assertEquals(1L, publisher.failed)
    }

    @Test
    fun `close closes the producer`() {
        val producer = MockProducer(true, null, StringSerializer(), StringSerializer())
        DeadLetterPublisher(producer).close()
        assertTrue(producer.closed())
    }

    private fun header(header: org.apache.kafka.common.header.Header?): String =
        String(header?.value() ?: ByteArray(0), StandardCharsets.UTF_8)
}
