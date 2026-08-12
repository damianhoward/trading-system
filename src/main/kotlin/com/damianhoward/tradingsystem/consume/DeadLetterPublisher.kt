package com.damianhoward.tradingsystem.consume

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** A dead-letter send the broker did not acknowledge; the source record must not be committed past. */
class DeadLetterPublishException(
    cause: Throwable,
) : RuntimeException("dead-letter publish was not acknowledged", cause)

/**
 * Publishes records the consumer has given up on to the dead-letter topic: the original key and
 * payload untouched, plus headers recording where the record came from and why it failed, so an
 * operator can inspect and replay it.
 *
 * [publish] returns only after the broker acknowledges the send, and throws
 * [DeadLetterPublishException] otherwise — the caller must not commit the source offset past a
 * record that is in neither stream. A record is either applied, or on the DLT, or still ahead
 * of the committed offset; it can never be in none of those places.
 */
class DeadLetterPublisher(
    private val producer: Producer<String, String>,
    private val topic: String = DEFAULT_TOPIC,
    private val confirmTimeout: Duration = DEFAULT_CONFIRM_TIMEOUT,
) : AutoCloseable {
    private val publishedCount = AtomicLong()
    private val failedCount = AtomicLong()

    /** Records acknowledged by the broker. */
    val published: Long get() = publishedCount.get()

    /** Sends that completed with an error (broker unreachable, timeout). */
    val failed: Long get() = failedCount.get()

    fun publish(
        record: ConsumerRecord<String, String>,
        error: Exception,
    ) {
        val out = ProducerRecord(topic, record.key(), record.value())
        out
            .headers()
            .add("dlt.error.class", utf8(error.javaClass.name))
            .add("dlt.error.message", utf8(error.message ?: ""))
            .add("dlt.source.topic", utf8(record.topic()))
            .add("dlt.source.partition", utf8(record.partition().toString()))
            .add("dlt.source.offset", utf8(record.offset().toString()))
        try {
            producer.send(out).get(confirmTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            failedCount.incrementAndGet()
            throw DeadLetterPublishException(e)
        } catch (e: Exception) {
            failedCount.incrementAndGet()
            throw DeadLetterPublishException(e)
        }
        publishedCount.incrementAndGet()
    }

    /** Flushes in-flight sends and closes the producer. */
    override fun close() {
        producer.close(CLOSE_TIMEOUT)
    }

    private fun utf8(s: String): ByteArray = s.toByteArray(StandardCharsets.UTF_8)

    companion object {
        const val DEFAULT_TOPIC = "orderbook.fills.DLT"
        val DEFAULT_CONFIRM_TIMEOUT: Duration = Duration.ofSeconds(10)
        private val CLOSE_TIMEOUT = Duration.ofSeconds(5)

        /** A publisher over a real [KafkaProducer], timeouts tightened so a dead broker surfaces in seconds. */
        fun create(
            bootstrapServers: String,
            topic: String = DEFAULT_TOPIC,
        ): DeadLetterPublisher {
            val props =
                Properties().apply {
                    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                    put(ProducerConfig.CLIENT_ID_CONFIG, "trading-system-dlt")
                    put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000)
                    put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000)
                    put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000)
                }
            return DeadLetterPublisher(KafkaProducer(props, StringSerializer(), StringSerializer()), topic)
        }
    }
}
