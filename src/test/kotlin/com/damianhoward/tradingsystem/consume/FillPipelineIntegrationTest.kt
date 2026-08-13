package com.damianhoward.tradingsystem.consume

import com.damianhoward.riskengine.report.RiskReportAssembler
import com.damianhoward.tradingsystem.capture.TradeCapture
import com.damianhoward.tradingsystem.limits.LimitKind
import com.damianhoward.tradingsystem.limits.LimitsChecker
import com.damianhoward.tradingsystem.limits.LimitsReport
import com.damianhoward.tradingsystem.limits.RiskLimits
import com.damianhoward.tradingsystem.position.Ledger
import com.damianhoward.tradingsystem.position.LedgerSnapshot
import com.damianhoward.tradingsystem.position.Position
import com.damianhoward.tradingsystem.position.PositionBook
import com.damianhoward.tradingsystem.position.PositionStore
import com.damianhoward.tradingsystem.position.RecordOutcome
import com.damianhoward.tradingsystem.position.SymbolTotals
import com.damianhoward.tradingsystem.pricing.MarketAssumptions
import com.damianhoward.tradingsystem.pricing.RiskGateway
import com.damianhoward.tradingsystem.web.Broadcaster
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * The whole consume pipeline against a real broker: fills produced onto the topic land as
 * positions, and a poison record lands on the DLT with its error headers while the stream keeps
 * moving; a second test proves fan-out — two independent consumers over one topic each receive
 * every fill. A stub cannot fail the way a broker fails (metadata, partitioning, wakeup
 * semantics), so this runs against the real thing; it skips only where Docker is absent and
 * always runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
class FillPipelineIntegrationTest {
    companion object {
        @Container
        @JvmField
        val kafka: KafkaContainer = KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))
    }

    private class CollectingStore : PositionStore {
        val applied = ConcurrentLinkedQueue<Pair<Fill, FillSource>>()
        private val ledger = ConcurrentHashMap<FillSource, Fill>()
        private val positions = ConcurrentHashMap<String, Position>()

        override fun record(
            fill: Fill,
            source: FillSource,
        ): RecordOutcome {
            if (ledger.putIfAbsent(source, fill) != null) return RecordOutcome.Duplicate
            val updated =
                positions.compute(fill.symbol) { _, current ->
                    Position(fill.symbol, (current?.quantity ?: 0L) + fill.signedSize, fill.price, fill.timeMillis)
                }!!
            applied.add(fill to source)
            return RecordOutcome.Applied(updated)
        }

        override fun loadAll(): List<Position> = positions.values.sortedBy { it.symbol }

        override fun loadLedger(topic: String): Ledger = Ledger(applied.map { it.first }, emptyMap())

        override fun ledgerSnapshot(topic: String): LedgerSnapshot =
            LedgerSnapshot(
                highWaterOffset = applied.map { it.second.offset }.maxOrNull(),
                totals = positions.values.map { SymbolTotals(it.symbol, it.quantity, it.quantity) },
            )

        override fun ping(): Boolean = true
    }

    private class SilentBroadcaster : Broadcaster {
        override fun startHeartbeat(periodSeconds: Long) {}

        override fun broadcast(json: String) {}

        override fun stream(
            exchange: com.sun.net.httpserver.HttpExchange,
            initialJson: String,
        ) = throw UnsupportedOperationException()
    }

    private fun fillJson(
        size: Long,
        aggressor: String = "BID",
        ts: Long = 1000,
    ): String =
        """{"v":1,"symbol":"SIM","price":"101.00000000","size":$size,""" +
            """"makerOrderId":1,"takerOrderId":2,"aggressor":"$aggressor","ts":$ts}"""

    private fun emptyLimits() = LimitsReport(RiskLimits(50, BigDecimal("5000")), emptyList(), emptyList(), 0)

    @Test
    fun `fills off the topic become positions and a poison record is dead-lettered, not fatal`() {
        val topic = "orderbook.fills"
        val dlt = "orderbook.fills.DLT"
        val store = CollectingStore()
        val book = PositionBook()
        val capture =
            TradeCapture(
                book,
                store,
                RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
                SilentBroadcaster(),
                limitsView = { emptyLimits() },
            )
        val deadLetters = DeadLetterPublisher.create(kafka.bootstrapServers, dlt)
        val consumer =
            FillConsumer.create(
                bootstrapServers = kafka.bootstrapServers,
                topic = topic,
                handler = RetryingHandler(capture, deadLetters, backoff = Duration.ofMillis(10)),
                startOffsets = emptyMap(),
                clientId = "trading-system-it",
            )

        val producerProps =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            }
        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            producer.send(ProducerRecord(topic, "SIM", fillJson(size = 5)))
            producer.send(ProducerRecord(topic, "SIM", """{"not":"a fill"}"""))
            producer.send(ProducerRecord(topic, "SIM", fillJson(size = 2, aggressor = "OFFER", ts = 2000)))
            producer.flush()
        }

        consumer.start()
        try {
            awaitTrue("both healthy fills should be booked") { book.positionOf("SIM")?.quantity == 3L }
            assertEquals(2, store.applied.size, "one ledger entry per healthy fill")
            assertTrue(
                store.applied.all { (_, source) -> source.topic == topic && source.offset >= 0 },
                "every ledger entry carries its stream coordinates",
            )

            // The poison record must be on the DLT with its provenance, original payload untouched.
            val dead = consumeOne(dlt)
            assertEquals("""{"not":"a fill"}""", dead.second)
            assertEquals("missing field: v", dead.first)
        } finally {
            consumer.close()
            deadLetters.close()
        }
    }

    @Test
    fun `two independent consumers over one topic each receive every fill`() {
        val topic = "orderbook.fills.fanout"
        val store = CollectingStore()
        val book = PositionBook()
        val limitsChecker = LimitsChecker(RiskLimits(maxAbsPosition = 3, maxNotional = BigDecimal("1000000")))
        val capture =
            TradeCapture(
                book,
                store,
                RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default()),
                SilentBroadcaster(),
                limitsChecker,
            )
        val deadLetters = DeadLetterPublisher.create(kafka.bootstrapServers, "$topic.DLT")
        // Both paths attach the way production does: no group, seeking from the ledger's
        // high-water mark — here empty, so the whole retained stream replays into each.
        val positionsConsumer =
            FillConsumer.create(
                bootstrapServers = kafka.bootstrapServers,
                topic = topic,
                handler = RetryingHandler(capture, deadLetters, backoff = Duration.ofMillis(10)),
                startOffsets = emptyMap(),
                clientId = "fanout-it-positions",
            )
        val limitsConsumer =
            FillConsumer.create(
                bootstrapServers = kafka.bootstrapServers,
                topic = topic,
                handler = limitsChecker,
                startOffsets = emptyMap(),
                clientId = "fanout-it-limits",
            )

        val producerProps =
            Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
            }
        KafkaProducer(producerProps, StringSerializer(), StringSerializer()).use { producer ->
            producer.send(ProducerRecord(topic, "SIM", fillJson(size = 2)))
            producer.send(ProducerRecord(topic, "SIM", fillJson(size = 3, ts = 2000)))
            producer.flush()
        }

        positionsConsumer.start()
        limitsConsumer.start()
        try {
            awaitTrue("the positions consumer should book both fills") { book.positionOf("SIM")?.quantity == 5L }
            awaitTrue("the limits consumer should see the same net position") {
                limitsChecker
                    .report()
                    .symbols
                    .singleOrNull()
                    ?.netQuantity == 5L
            }

            // Same two fills, consumed independently by each path: the limits side crossed its
            // own ceiling on the second fill.
            val report = limitsChecker.report()
            assertTrue(report.symbols.single().breached, "net 5 over a ceiling of 3")
            val event = report.events.single()
            assertEquals(LimitKind.POSITION, event.kind)
            assertTrue(event.breached)
            assertEquals(2000, event.timeMillis)
        } finally {
            positionsConsumer.close()
            limitsConsumer.close()
            deadLetters.close()
        }
    }

    private fun awaitTrue(
        message: String,
        condition: () -> Boolean,
    ) {
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (!condition()) {
            assertTrue(System.nanoTime() < deadline, message)
            Thread.sleep(100)
        }
    }

    /** The first DLT record's (error message header, value). */
    private fun consumeOne(topic: String): Pair<String, String> {
        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers)
                put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-inspector")
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            }
        KafkaConsumer(props, StringDeserializer(), StringDeserializer()).use { consumer ->
            consumer.subscribe(listOf(topic))
            val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
            while (System.nanoTime() < deadline) {
                val records = consumer.poll(Duration.ofMillis(500))
                if (!records.isEmpty) {
                    val record = records.first()
                    val error = String(record.headers().lastHeader("dlt.error.message").value(), StandardCharsets.UTF_8)
                    return error to record.value()
                }
            }
        }
        throw AssertionError("nothing arrived on $topic within 30s")
    }
}
