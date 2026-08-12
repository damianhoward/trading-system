package com.damianhoward.tradingsystem.limits

import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.consume.Fill
import com.damianhoward.tradingsystem.consume.RecordHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.math.abs

/** What the dashboard reads; [LimitsChecker] is the production implementation. */
fun interface LimitsView {
    fun report(): LimitsReport
}

/**
 * The limits consumer's whole record policy: an independent view of the fill stream that derives
 * its own net position per symbol — it never reads the position book — and records a
 * [BreachEvent] whenever exposure crosses a [RiskLimits] ceiling in either direction. Checking
 * is in-memory only, so there is no transient failure to retry; a malformed record is counted
 * and skipped, not dead-lettered — the positions consumer owns the DLT, and a second publisher
 * would duplicate every poison record.
 *
 * Restart safety comes from the fill ledger, not Kafka group offsets: [warm] replays the
 * persisted fills at startup (events carry each fill's own timestamp, so the rebuilt history is
 * the same one), and the consumer then seeks the live stream from the ledger's high-water mark.
 * Thread-safe: the consumer thread writes, web threads read.
 */
class LimitsChecker(
    private val limits: RiskLimits,
) : RecordHandler,
    LimitsView {
    private class Exposure(
        var netQuantity: Long = 0,
        var lastPrice: BigDecimal = BigDecimal.ZERO,
        var positionBreached: Boolean = false,
        var notionalBreached: Boolean = false,
    )

    private val lock = Any()
    private val exposures = HashMap<String, Exposure>()
    private val events = ArrayDeque<BreachEvent>()
    private var malformed = 0L

    /** Where this view sits on the stream — the readiness probe compares it with the positions view. */
    @Volatile
    var progress: ConsumerProgress? = null
        private set

    @Volatile
    private var listener: () -> Unit = {}

    /** Called after every handled record, applied or counted — set once at wiring time. */
    fun onChange(listener: () -> Unit) {
        this.listener = listener
    }

    /**
     * Rebuilds exposure state from the persisted ledger before the live stream attaches —
     * startup only, before the consumer thread exists. [ledgerProgress] is the ledger's
     * high-water mark, so the report's stream position is truthful from the first render.
     */
    fun warm(
        fills: List<Fill>,
        ledgerProgress: ConsumerProgress?,
    ) {
        synchronized(lock) {
            fills.forEach(::apply)
            progress = ledgerProgress
        }
    }

    override fun handle(record: ConsumerRecord<String, String>) {
        val fill =
            try {
                Fill.parse(record.value())
            } catch (_: IllegalArgumentException) {
                synchronized(lock) { malformed++ }
                listener()
                return
            }
        synchronized(lock) {
            apply(fill)
            progress = ConsumerProgress(record.offset(), fill.timeMillis)
        }
        listener()
    }

    private fun apply(fill: Fill) {
        val exposure = exposures.getOrPut(fill.symbol) { Exposure() }
        exposure.netQuantity += fill.signedSize
        exposure.lastPrice = fill.price
        val absQuantity = BigDecimal(abs(exposure.netQuantity))
        val positionNow = abs(exposure.netQuantity) > limits.maxAbsPosition
        if (positionNow != exposure.positionBreached) {
            exposure.positionBreached = positionNow
            record(fill, LimitKind.POSITION, positionNow, absQuantity, BigDecimal(limits.maxAbsPosition))
        }
        val notional = absQuantity * exposure.lastPrice
        val notionalNow = notional > limits.maxNotional
        if (notionalNow != exposure.notionalBreached) {
            exposure.notionalBreached = notionalNow
            record(fill, LimitKind.NOTIONAL, notionalNow, notional, limits.maxNotional)
        }
    }

    private fun record(
        fill: Fill,
        kind: LimitKind,
        breached: Boolean,
        value: BigDecimal,
        limit: BigDecimal,
    ) {
        events.addFirst(BreachEvent(fill.symbol, kind, breached, value, limit, fill.timeMillis))
        while (events.size > MAX_EVENTS) events.removeLast()
    }

    override fun report(): LimitsReport =
        synchronized(lock) {
            LimitsReport(
                limits = limits,
                symbols = exposures.entries.sortedBy { it.key }.map { (symbol, exposure) -> symbolLimits(symbol, exposure) },
                events = events.toList(),
                malformed = malformed,
                progress = progress,
            )
        }

    private fun symbolLimits(
        symbol: String,
        exposure: Exposure,
    ): SymbolLimits {
        val absQuantity = BigDecimal(abs(exposure.netQuantity))
        val notional = absQuantity * exposure.lastPrice
        return SymbolLimits(
            symbol = symbol,
            netQuantity = exposure.netQuantity,
            lastPrice = exposure.lastPrice,
            notional = notional,
            positionUtilisation = absQuantity.divide(BigDecimal(limits.maxAbsPosition), 4, RoundingMode.HALF_UP),
            notionalUtilisation = notional.divide(limits.maxNotional, 4, RoundingMode.HALF_UP),
            breached = exposure.positionBreached || exposure.notionalBreached,
        )
    }

    companion object {
        const val MAX_EVENTS = 20
    }
}
