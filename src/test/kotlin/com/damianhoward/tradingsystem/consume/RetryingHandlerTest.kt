package com.damianhoward.tradingsystem.consume

import com.damianhoward.tradingsystem.capture.FillHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration

class RetryingHandlerTest {
    private val goodJson =
        """{"v":1,"symbol":"SIM","price":"101.00000000","size":5,""" +
            """"makerOrderId":12,"takerOrderId":34,"aggressor":"BID","ts":1000}"""

    private fun record(value: String) = ConsumerRecord("orderbook.fills", 0, 7L, "SIM", value)

    private class CountingHandler(
        private val failures: Int,
    ) : FillHandler {
        val fills = mutableListOf<Fill>()
        val sources = mutableListOf<FillSource>()
        var calls = 0

        override fun onFill(
            fill: Fill,
            source: FillSource,
        ) {
            calls++
            if (calls <= failures) throw IllegalStateException("transient failure $calls")
            fills.add(fill)
            sources.add(source)
        }
    }

    private fun publisher(producer: MockProducer<String, String>) = DeadLetterPublisher(producer, "orderbook.fills.DLT")

    private fun mockProducer() = MockProducer(true, null, StringSerializer(), StringSerializer())

    @Test
    fun `a healthy record is handled once with its stream coordinates, no retries, no dead letters`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 0)
        val sleeps = mutableListOf<Duration>()

        RetryingHandler(handler, publisher(producer), attempts = 3, sleep = sleeps::add).handle(record(goodJson))

        assertEquals(1, handler.calls)
        assertEquals("SIM", handler.fills.single().symbol)
        assertEquals(FillSource("orderbook.fills", 0, 7L), handler.sources.single())
        assertTrue(sleeps.isEmpty())
        assertTrue(producer.history().isEmpty())
    }

    @Test
    fun `a transient failure is retried with backoff and recovers`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 2)
        val sleeps = mutableListOf<Duration>()
        val backoff = Duration.ofMillis(250)

        RetryingHandler(handler, publisher(producer), attempts = 3, backoff = backoff, sleep = sleeps::add).handle(record(goodJson))

        assertEquals(3, handler.calls)
        assertEquals(1, handler.fills.size, "the third attempt should have succeeded")
        assertEquals(listOf(backoff, backoff), sleeps)
        assertTrue(producer.history().isEmpty(), "a recovered record must not be dead-lettered")
    }

    @Test
    fun `exhausted retries halt the stream — a valid fill is never dead-lettered and skipped`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 99)

        val thrown =
            assertThrows(FillRetriesExhaustedException::class.java) {
                RetryingHandler(handler, publisher(producer), attempts = 3, sleep = {}).handle(record(goodJson))
            }

        assertEquals(3, handler.calls, "no attempts beyond the configured bound")
        assertTrue(
            producer.history().isEmpty(),
            "the fill must stay ahead of the committed offset for the restart replay, not move to the DLT",
        )
        assertEquals("transient failure 3", thrown.cause?.message, "the final attempt's error is the cause")
        assertTrue(thrown.message!!.contains("orderbook.fills-0@7"), "the halt names the stuck coordinates")
    }

    @Test
    fun `a malformed record is dead-lettered immediately, never retried`() {
        val producer = mockProducer()
        val handler = CountingHandler(failures = 0)
        val sleeps = mutableListOf<Duration>()

        RetryingHandler(handler, publisher(producer), attempts = 3, sleep = sleeps::add).handle(record("not json"))

        assertEquals(0, handler.calls, "malformed input never reaches the handler")
        assertTrue(sleeps.isEmpty(), "malformed input never heals, so retrying it is pure stall")
        assertEquals("not json", producer.history().single().value())
    }

    @Test
    fun `an unacknowledged dead-letter send propagates — the record must not be lost silently`() {
        // Auto-complete off and a tiny confirm window: the send never acks, publish throws.
        val producer = MockProducer(false, null, StringSerializer(), StringSerializer())
        val stalled = DeadLetterPublisher(producer, "orderbook.fills.DLT", confirmTimeout = Duration.ofMillis(50))
        val handler = CountingHandler(failures = 0)

        assertThrows(DeadLetterPublishException::class.java) {
            RetryingHandler(handler, stalled, attempts = 1, sleep = {}).handle(record("not json"))
        }
        assertEquals(0, handler.calls, "poison never reaches the handler")
        assertEquals(1, stalled.failed, "the failure is counted for the readiness probe")
    }

    @Test
    fun `a dead-letter failure from inside the handler is not treated as retriable`() {
        val handler =
            FillHandler { _, _ -> throw DeadLetterPublishException(IllegalStateException("no ack")) }
        val sleeps = mutableListOf<Duration>()

        assertThrows(DeadLetterPublishException::class.java) {
            RetryingHandler(handler, publisher(mockProducer()), attempts = 3, sleep = sleeps::add).handle(record(goodJson))
        }
        assertTrue(sleeps.isEmpty(), "an unacknowledged dead letter halts; retrying would risk committing past it")
    }

    @Test
    fun `attempts below one are rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            RetryingHandler(CountingHandler(0), publisher(mockProducer()), attempts = 0)
        }
    }
}
