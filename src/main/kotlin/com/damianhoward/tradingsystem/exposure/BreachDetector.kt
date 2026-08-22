package com.damianhoward.tradingsystem.exposure

import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.consume.Fill
import com.damianhoward.tradingsystem.consume.RecordHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.math.abs

/** What the dashboard reads; [BreachDetector] is the production implementation. */
fun interface ExposureView {
    fun report(): ExposureReport
}

/**
 * **This detects breaches. It does not prevent them.** Every fill it sees has already happened, so
 * a ceiling crossed here is a fact being reported, not an order being refused — nothing in the
 * estate rejects an order for exceeding a limit, and the order book's only submission control is a
 * per-IP token bucket. Read [RiskLimits] as ceilings that are *watched*, and do not let the word
 * "limit" imply a control that is not there.
 *
 * It still earns its place for two reasons a pre-trade control could not cover. Notional is
 * |net quantity| × last price, so it can breach with **no trade at all** — the price moves and the
 * ceiling arrives on its own, which nothing checking orders would ever see. And deriving exposure
 * independently is what makes it a check on a control rather than a restatement of one: the day
 * something does gate orders, this is the thing that can say whether the gate held.
 *
 * The independence is the design. It derives its own net position per symbol and **never reads the
 * position book**, so the two can disagree and readiness gates on them agreeing. It records a
 * [BreachEvent] whenever exposure crosses a ceiling in either direction. Detection is in-memory
 * only, so there is no transient failure to retry; a malformed record is counted and skipped, not
 * dead-lettered — the positions consumer owns the DLT, and a second publisher would duplicate
 * every poison record.
 *
 * Restart safety comes from the fill ledger, not Kafka group offsets: [warm] replays the
 * persisted fills at startup (events carry each fill's own timestamp, so the rebuilt history is
 * the same one), and the consumer then seeks the live stream from the ledger's high-water mark.
 * Thread-safe: the consumer thread writes, web threads read.
 */
class BreachDetector(
    private val limits: RiskLimits,
) : RecordHandler,
    ExposureView {
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
    private var breaches = 0L

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
        // Counted as well as recorded, because the deque forgets. It holds MAX_EVENTS and drops the
        // oldest, so a breach that happened is erased by twenty later transitions and a restart
        // takes the rest — which left the only durable trace of a breach being whoever happened to
        // have the dashboard open. A monotonic count cannot be evicted, so an alert can fire on the
        // increase long after the event itself has gone.
        if (breached) breaches++
    }

    override fun report(): ExposureReport =
        synchronized(lock) {
            ExposureReport(
                limits = limits,
                symbols = exposures.entries.sortedBy { it.key }.map { (symbol, exposure) -> symbolExposure(symbol, exposure) },
                events = events.toList(),
                malformed = malformed,
                breaches = breaches,
                progress = progress,
            )
        }

    private fun symbolExposure(
        symbol: String,
        exposure: Exposure,
    ): SymbolExposure {
        val absQuantity = BigDecimal(abs(exposure.netQuantity))
        val notional = absQuantity * exposure.lastPrice
        return SymbolExposure(
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
