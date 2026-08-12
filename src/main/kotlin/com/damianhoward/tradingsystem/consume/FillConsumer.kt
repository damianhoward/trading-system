package com.damianhoward.tradingsystem.consume

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties

/**
 * The poll loop over the fills topic, on its own thread, reporting into [ConsumerHealth].
 *
 * There is no consumer group and no offset commit: the fill ledger is the only checkpoint. Each
 * partition is assigned directly and starts just past its recorded offset ([startOffsets], the
 * ledger's high-water marks) — or from the beginning where nothing is recorded, so a fresh or
 * rebuilt database re-derives its state from the retained stream. Delivery is at-least-once by
 * construction (a crash between apply and the next poll replays the tail) and idempotent
 * application makes the replay safe. A group commit would only add a second, competing account
 * of how far this path has read.
 *
 * An exception the [RecordHandler] lets through is fatal by design: a valid record exhausted
 * its retries or a dead-letter send went unacknowledged — either way the record is applied,
 * dead-lettered, or still ahead of the ledger's high-water mark, never lost. The loop marks the
 * health record failed and hands the error to [onFatal] — production exits the process and
 * systemd restarts it into a safe replay.
 */
class FillConsumer(
    private val consumer: Consumer<String, String>,
    private val topic: String,
    private val handler: RecordHandler,
    val health: ConsumerHealth,
    private val startOffsets: Map<Int, Long>,
    private val pollTimeout: Duration = Duration.ofMillis(500),
    private val onFatal: (Exception) -> Unit = {},
    threadName: String = "fill-consumer",
) : AutoCloseable {
    private val thread = Thread(::run, threadName).apply { isDaemon = true }

    @Volatile
    private var running = false

    fun start() {
        running = true
        thread.start()
    }

    private fun run() {
        health.started()
        try {
            attach()
            while (running) {
                val records = consumer.poll(pollTimeout)
                health.polled()
                health.assigned(consumer.assignment().size)
                records.forEach(handler::handle)
            }
            health.stopped()
        } catch (_: WakeupException) {
            // close() woke a blocked poll — fall through to cleanup.
            health.stopped()
        } catch (e: Exception) {
            health.failed(e)
            onFatal(e)
        } finally {
            consumer.close()
        }
    }

    /** Assigns every partition and seeks past its recorded offset, or to the beginning without one. */
    private fun attach() {
        val partitions = consumer.partitionsFor(topic).map { TopicPartition(topic, it.partition()) }
        consumer.assign(partitions)
        partitions.forEach { partition ->
            val applied = startOffsets[partition.partition()]
            if (applied != null) {
                consumer.seek(partition, applied + 1)
            } else {
                consumer.seekToBeginning(listOf(partition))
            }
        }
    }

    /**
     * Stops the loop and waits for it to finish. [Consumer] is single-threaded by contract;
     * `wakeup()` is its one thread-safe method and the documented way to abort a blocked poll,
     * so the consumer itself is always closed on the poll thread.
     */
    override fun close() {
        running = false
        consumer.wakeup()
        thread.join(CLOSE_TIMEOUT.toMillis())
    }

    companion object {
        private val CLOSE_TIMEOUT = Duration.ofSeconds(10)

        /**
         * A consumer over a real [KafkaConsumer], starting past [startOffsets]. [clientId] must
         * be unique per consumer in the process — it names the poll thread and the JMX metrics,
         * and two consumers sharing one collide on registration.
         */
        fun create(
            bootstrapServers: String,
            topic: String,
            handler: RecordHandler,
            startOffsets: Map<Int, Long>,
            clientId: String,
            onFatal: (Exception) -> Unit = {},
        ): FillConsumer =
            FillConsumer(
                consumer = kafkaConsumer(bootstrapServers, clientId),
                topic = topic,
                handler = handler,
                health = ConsumerHealth(clientId),
                startOffsets = startOffsets,
                onFatal = onFatal,
                threadName = clientId,
            )

        private fun kafkaConsumer(
            bootstrapServers: String,
            clientId: String,
        ): Consumer<String, String> {
            val props =
                Properties().apply {
                    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                    put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
                    // Auto-commit defaults to true and is rejected without a group id; there is
                    // nothing to commit to — the ledger is the checkpoint.
                    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                }
            return KafkaConsumer(props, StringDeserializer(), StringDeserializer())
        }
    }
}
