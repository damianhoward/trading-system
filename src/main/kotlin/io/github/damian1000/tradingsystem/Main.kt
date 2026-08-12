package io.github.damian1000.tradingsystem

import io.github.damian1000.riskengine.report.RiskReportAssembler
import io.github.damian1000.tradingsystem.capture.SessionOpens
import io.github.damian1000.tradingsystem.capture.TradeCapture
import io.github.damian1000.tradingsystem.config.AppConfig
import io.github.damian1000.tradingsystem.consume.ConsumerProgress
import io.github.damian1000.tradingsystem.consume.DeadLetterPublisher
import io.github.damian1000.tradingsystem.consume.DltReplay
import io.github.damian1000.tradingsystem.consume.FillConsumer
import io.github.damian1000.tradingsystem.consume.RetryingHandler
import io.github.damian1000.tradingsystem.health.Readiness
import io.github.damian1000.tradingsystem.limits.LimitsChecker
import io.github.damian1000.tradingsystem.limits.RiskLimits
import io.github.damian1000.tradingsystem.position.JdbcPositionStore
import io.github.damian1000.tradingsystem.position.PositionBook
import io.github.damian1000.tradingsystem.position.ReconciliationChecker
import io.github.damian1000.tradingsystem.pricing.MarketAssumptions
import io.github.damian1000.tradingsystem.pricing.RiskGateway
import io.github.damian1000.tradingsystem.web.DashboardServer
import io.github.damian1000.tradingsystem.web.SseBroadcaster
import io.github.damian1000.tradingsystem.web.WebAssets
import org.flywaydb.core.Flyway
import java.sql.DriverManager
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

/**
 * Composition root: reads config, migrates the schema, warms both views from the fill ledger,
 * and wires fills → positions → risk → dashboard plus the independent limits view over the same
 * topic, with a periodic check that the book still equals the fills behind it. Plumbing only —
 * every collaborator is constructed here and tested elsewhere.
 *
 * A consumer that dies on an unexpected exception exits the process: continuing would mean
 * committing past records that are in neither the ledger nor the DLT, and systemd's
 * `Restart=on-failure` brings the process back into a replay the ledger makes idempotent.
 *
 * `replay-dlt` runs [DltReplay] instead of the service — the operator entry point for feeding
 * recoverable dead letters back through the pipeline (runbook: workspace-config doc 15).
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
    val limits = LimitsChecker(RiskLimits(config.limitMaxPosition, config.limitMaxNotional))
    limits.warm(ledger.fills, ledgerProgress)
    val deadLetters = DeadLetterPublisher.create(config.kafkaBootstrapServers, config.deadLetterTopic)
    val opens = SessionOpens().apply { ledger.fills.forEach(::observe) }
    val capture = TradeCapture(book, store, risk, broadcaster, limits, opens, ledgerProgress, deadLetters::published)
    limits.onChange { broadcaster.broadcast(capture.snapshot().toJson()) }

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
    val limitsConsumer =
        FillConsumer.create(
            bootstrapServers = config.kafkaBootstrapServers,
            topic = config.fillsTopic,
            handler = limits,
            startOffsets = ledger.highWaterOffsets,
            clientId = "trading-system-limits",
            onFatal = fatal,
        )

    // One pass before the server starts, so the probe is never answering from an empty result:
    // an unpopulated check and a failing one are the same 503, and starting into it would make
    // every deploy look like a divergence for the first interval.
    val reconciler = ReconciliationChecker(store::symbolTotals).apply { run() }
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
            consumers = listOf(consumer.health, limitsConsumer.health),
            databaseOk = store::ping,
            deadLettersPublished = deadLetters::published,
            deadLettersFailed = deadLetters::failed,
            positionsView = capture::progress,
            limitsView = limits::progress,
            reconciliation = reconciler::latest,
        )
    val server = DashboardServer(capture, broadcaster, WebAssets.load(), config.port, readiness)

    Runtime.getRuntime().addShutdownHook(
        Thread {
            consumer.close()
            limitsConsumer.close()
            reconciliations.shutdownNow()
            deadLetters.close()
            broadcaster.close()
            server.stop()
        },
    )

    server.start()
    consumer.start()
    limitsConsumer.start()
}
