package com.damianhoward.tradingsystem.consume

import com.damianhoward.tradingsystem.config.AppConfig
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * One-shot operator tool (`trading-system replay-dlt`): replays dead-lettered records that now
 * parse back onto the fills topic, so a fill dead-lettered by a since-fixed defect re-enters
 * the pipeline. Reads the DLT from the beginning up to where it ended at attach — records
 * dead-lettered mid-replay wait for the next run — republishing each record whose payload now
 * parses with its key, payload, and `dlt.*` provenance headers untouched, and leaving
 * still-malformed records where they are.
 *
 * Every send is confirmed before the next record is read, and each outcome is logged with its
 * DLT coordinates; a failed send aborts the run. A replayed copy lands at new stream coordinates,
 * so the ledger's coordinate key cannot recognise it as a duplicate; the `exec_id` unique index
 * can, and does for every record carrying one. A record whose payload has no `execId` — the
 * producer made it optional — still applies twice if the tool is run twice. The runbook therefore
 * treats replay as a deliberate, once-per-incident operation.
 */
class DltReplay(
    private val consumer: Consumer<String, String>,
    private val producer: Producer<String, String>,
    private val dltTopic: String,
    private val fillsTopic: String,
    private val confirmTimeout: Duration = DEFAULT_CONFIRM_TIMEOUT,
    private val pollTimeout: Duration = Duration.ofMillis(500),
    private val log: (String) -> Unit = ::println,
) : AutoCloseable {
    /** What one run did: [replayed] records re-entered the stream, [stillMalformed] stayed dead. */
    data class Summary(
        val replayed: Int,
        val stillMalformed: Int,
    )

    fun run(): Summary {
        val partitions =
            (consumer.partitionsFor(dltTopic) ?: emptyList())
                .map { TopicPartition(dltTopic, it.partition()) }
        if (partitions.isEmpty()) return Summary(0, 0)
        consumer.assign(partitions)
        consumer.seekToBeginning(partitions)
        val end = consumer.endOffsets(partitions)
        var replayed = 0
        var stillMalformed = 0
        while (partitions.any { consumer.position(it) < end.getValue(it) }) {
            for (record in consumer.poll(pollTimeout)) {
                if (record.offset() >= end.getValue(TopicPartition(record.topic(), record.partition()))) continue
                val coordinates = "${record.topic()}-${record.partition()}@${record.offset()}"
                try {
                    Fill.parse(record.value())
                } catch (e: IllegalArgumentException) {
                    stillMalformed++
                    log("left still-malformed $coordinates: ${e.message}")
                    continue
                }
                val out = ProducerRecord(fillsTopic, record.key(), record.value())
                record.headers().forEach { out.headers().add(it) }
                producer.send(out).get(confirmTimeout.toMillis(), TimeUnit.MILLISECONDS)
                replayed++
                log("replayed $coordinates -> $fillsTopic")
            }
        }
        return Summary(replayed, stillMalformed)
    }

    override fun close() {
        consumer.close()
        producer.close(CLOSE_TIMEOUT)
    }

    companion object {
        val DEFAULT_CONFIRM_TIMEOUT: Duration = Duration.ofSeconds(10)
        private val CLOSE_TIMEOUT = Duration.ofSeconds(5)

        /** A replay over real Kafka clients, timeouts tightened like the service's own clients. */
        fun create(config: AppConfig): DltReplay {
            val consumerProps =
                Properties().apply {
                    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers)
                    put(ConsumerConfig.CLIENT_ID_CONFIG, "trading-system-dlt-replay")
                    put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                    put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                }
            val producerProps =
                Properties().apply {
                    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers)
                    put(ProducerConfig.CLIENT_ID_CONFIG, "trading-system-dlt-replay-producer")
                    put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000)
                    put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000)
                    put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000)
                }
            return DltReplay(
                consumer = KafkaConsumer(consumerProps, StringDeserializer(), StringDeserializer()),
                producer = KafkaProducer(producerProps, StringSerializer(), StringSerializer()),
                dltTopic = config.deadLetterTopic,
                fillsTopic = config.fillsTopic,
            )
        }
    }
}
