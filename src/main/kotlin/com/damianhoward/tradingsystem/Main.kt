package com.damianhoward.tradingsystem

import com.damianhoward.riskengine.report.RiskReportAssembler
import com.damianhoward.tradingsystem.capture.SessionOpens
import com.damianhoward.tradingsystem.capture.TradeCapture
import com.damianhoward.tradingsystem.config.AppConfig
import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.consume.DeadLetterPublisher
import com.damianhoward.tradingsystem.consume.DltReplay
import com.damianhoward.tradingsystem.consume.FillConsumer
import com.damianhoward.tradingsystem.consume.RetryingHandler
import com.damianhoward.tradingsystem.exposure.BreachDetector
import com.damianhoward.tradingsystem.exposure.RiskLimits
import com.damianhoward.tradingsystem.health.Readiness
import com.damianhoward.tradingsystem.position.JdbcPositionStore
import com.damianhoward.tradingsystem.position.PositionBook
import com.damianhoward.tradingsystem.position.ReconciliationChecker
import com.damianhoward.tradingsystem.position.View
import com.damianhoward.tradingsystem.position.ViewTotals
import com.damianhoward.tradingsystem.pricing.MarketAssumptions
import com.damianhoward.tradingsystem.pricing.RiskGateway
import com.damianhoward.tradingsystem.web.DashboardServer
import com.damianhoward.tradingsystem.web.SseBroadcaster
import com.damianhoward.tradingsystem.web.WebAssets
import org.flywaydb.core.Flyway
import java.sql.DriverManager
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * Composition root: reads config, migrates the schema, warms both views from the fill ledger,
 * and wires fills → positions → risk → dashboard plus the independent exposure view over the same
 * topic, with a periodic check that the book still equals the fills behind it. Plumbing only —
 * every collaborator is constructed here and tested elsewhere.
 *
 * A consumer that dies on an unexpected exception exits the process: continuing would mean
 * committing past records that are in neither the ledger nor the DLT, and systemd's
 * `Restart=on-failure` brings the process back into a replay the ledger makes idempotent.
 *
 * `replay-dlt` runs [DltReplay] instead of the service — the operator entry point for feeding
 * recoverable dead letters back through the pipeline.
 */
fun main(args: Array<String>) {
    val config = AppConfig.fromEnv(System.getenv())

    if (args.firstOrNull() == "replay-dlt") {
        val summary = DltReplay.create(config).use { it.run() }
        println(
            "replayed ${summary.replayed} record(s) to ${config.fillsTopic}; " +
                "${summary.stillMalformed} still-malformed record(s) left on ${config.deadLetterTopic}",
        )
        return
    }

    Flyway
        .configure()
        .dataSource(config.dbUrl, config.dbUser, config.dbPassword)
        .load()
        .migrate()

    val store = JdbcPositionStore { DriverManager.getConnection(config.dbUrl, config.dbUser, config.dbPassword) }
    val ledger = store.loadLedger(config.fillsTopic)
    val ledgerProgress =
        ledger.highWaterOffsets.values.maxOrNull()?.let { offset ->
            ConsumerProgress(offset, ledger.fills.last().timeMillis)
        }

    val book = PositionBook().apply { restore(store.loadAll()) }
    val broadcaster = SseBroadcaster().apply { startHeartbeat() }
    val risk = RiskGateway(RiskReportAssembler.standard(), MarketAssumptions.default())
    val detector = BreachDetector(RiskLimits(config.limitMaxPosition, config.limitMaxNotional))
    detector.warm(ledger.fills, ledgerProgress)
    val deadLetters = DeadLetterPublisher.create(config.kafkaBootstrapServers, config.deadLetterTopic)
    val opens = SessionOpens().apply { ledger.fills.forEach(::observe) }
    val capture = TradeCapture(book, store, risk, broadcaster, detector, opens, ledgerProgress, deadLetters::published)
    detector.onChange { broadcaster.broadcast(capture.snapshot().toJson()) }

    val fatal: (Exception) -> Unit = { error ->
        System.err.println("fill consumer failed:")
        error.printStackTrace()
        exitProcess(1)
    }
    val consumer =
        FillConsumer.create(
            bootstrapServers = config.kafkaBootstrapServers,
            topic = config.fillsTopic,
            handler = RetryingHandler(capture, deadLetters),
            startOffsets = ledger.highWaterOffsets,
            clientId = "trading-system-fills",
            onFatal = fatal,
        )
    val exposureConsumer =
        FillConsumer.create(
            bootstrapServers = config.kafkaBootstrapServers,
            topic = config.fillsTopic,
            handler = detector,
            startOffsets = ledger.highWaterOffsets,
            clientId = "trading-system-exposure",
            onFatal = fatal,
        )

    // One pass before the server starts, so the probe is never answering from an empty result:
    // an unpopulated check and a failing one are the same 503, and starting into it would make
    // every deploy look like a divergence for the first interval.
    //
    // Both in-memory views are warmed from the same ledger this pass reads, so they start at its
    // high-water offset and the first verdict is a judgement rather than an inconclusive one.
    val reconciler =
        ReconciliationChecker(
            ledgerSnapshot = { store.ledgerSnapshot(config.fillsTopic) },
            inMemoryViews =
                mapOf(
                    View.POSITION_BOOK to {
                        ViewTotals(capture.progress?.offset, book.all().associate { it.symbol to it.quantity })
                    },
                    View.EXPOSURE to {
                        val report = detector.report()
                        ViewTotals(report.progress?.offset, report.symbols.associate { it.symbol to it.netQuantity })
                    },
                ),
        ).apply { run() }
    val reconciliations =
        Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "position-reconciler").apply { isDaemon = true }
        }
    reconciliations.scheduleWithFixedDelay(
        reconciler::run,
        ReconciliationChecker.INTERVAL.toMillis(),
        ReconciliationChecker.INTERVAL.toMillis(),
        TimeUnit.MILLISECONDS,
    )

    val readiness =
        Readiness(
            consumers = listOf(consumer.health, exposureConsumer.health),
            databaseOk = store::ping,
            deadLettersPublished = deadLetters::published,
            deadLettersFailed = deadLetters::failed,
            positionsView = capture::progress,
            exposureReport = detector::report,
            reconciliation = reconciler::latest,
        )
    val server = DashboardServer(capture, broadcaster, WebAssets.load(), config.port, readiness)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            consumer.close()
            exposureConsumer.close()
            reconciliations.shutdownNow()
            deadLetters.close()
            broadcaster.close()
            server.stop()
        },
    )

    server.start()
    consumer.start()
    exposureConsumer.start()
}
