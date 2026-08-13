package com.damianhoward.tradingsystem.position

/**
 * One symbol's two quantities, as a single consistent read produced them: the signed sum of its
 * ledger fills, and the position row derived from those same fills.
 *
 * Both come from one SQL statement rather than two queries, so they describe the same instant.
 * Read separately, a fill landing between them would show up here as a divergence that never
 * existed — and a reconciliation that cries wolf is turned off, which costs more than never
 * having written it.
 */
data class SymbolTotals(
    val symbol: String,
    val ledgerQuantity: Long,
    val positionQuantity: Long,
)

/**
 * The ledger read once, for every view to be judged against: the per-symbol totals plus the
 * highest offset the ledger holds for the consumed topic.
 *
 * The offset is what makes an in-memory view comparable at all. A projection living in this
 * process cannot share the database's read-consistent snapshot, so "the same instant" has to be
 * expressed as "the same amount of stream" instead — see [Reconciliation.of].
 */
data class LedgerSnapshot(
    val highWaterOffset: Long?,
    val totals: List<SymbolTotals>,
) {
    val ledgerQuantities: Map<String, Long> get() = totals.associate { it.symbol to it.ledgerQuantity }
}

/**
 * What an in-memory projection reports about itself: how far along the stream it has read, and
 * its net quantity per symbol. Null [offset] means it has applied nothing yet.
 */
data class ViewTotals(
    val offset: Long?,
    val quantities: Map<String, Long>,
)

/**
 * A projection of the fill stream that the conservation check judges. A fixed set, which is what
 * lets the metrics label be bounded by configuration rather than by data.
 *
 * [POSITIONS] is the `positions` table; [POSITION_BOOK] is the in-memory mirror the dashboard and
 * the risk report are priced from; [LIMITS] is the limits consumer's independently-derived
 * exposure map.
 */
enum class View(
    val label: String,
) {
    POSITIONS("positions"),
    POSITION_BOOK("position_book"),
    LIMITS("limits"),
}

/**
 * A symbol whose view quantity does not equal the fills it is derived from.
 *
 * Both quantities are carried rather than only the difference, because the two failures look
 * different in the numbers: a view that drifted from a correct ledger is a projection bug, and a
 * ledger short of the view is missing history.
 */
data class Divergence(
    val symbol: String,
    val ledgerQuantity: Long,
    val viewQuantity: Long,
) {
    /** View minus ledger. Positive means the view claims more than the fills justify. */
    val difference: Long get() = viewQuantity - ledgerQuantity
}

/**
 * Why a view was not judged this pass: it had read a different amount of stream than the ledger
 * snapshot it would have been compared against.
 *
 * This is a deliberate outcome, not a failure to handle. Comparing an in-memory view against a
 * ledger it has not caught up with would report a divergence for every fill in flight, and a
 * check that cries wolf gets turned off. Both offsets are carried so an operator can see which
 * side is behind.
 *
 * It is not a free pass. A view that can never be judged is as useless as one that disagrees, so
 * [com.damianhoward.tradingsystem.health.Readiness] fails once a view has been continuously
 * inconclusive past its budget — the same shape as the coherence grace, and for the same reason.
 */
data class Inconclusive(
    val viewOffset: Long?,
    val ledgerOffset: Long?,
)

/**
 * One view's standing against the ledger: either a judgement ([divergences], possibly empty) or
 * an [inconclusive] pass. Exactly one of the two is meaningful.
 */
data class ViewVerdict(
    val view: View,
    val symbolsChecked: Int,
    val divergences: List<Divergence>,
    val inconclusive: Inconclusive? = null,
) {
    /** True when this view was judged and agreed. An inconclusive pass is neither agreement nor divergence. */
    val agrees: Boolean get() = inconclusive == null && divergences.isEmpty()
}

/**
 * The result of one pass over every derived view, asserting the conservation property that each
 * one equals the signed sum of the ledger fills behind it.
 *
 * The write path maintains the `positions` table by construction — [PositionStore.record] inserts
 * the fill and moves the position in one transaction — which is exactly why it is worth checking.
 * Nothing in the service reads the ledger to serve a request, so a projection that has drifted
 * from it is invisible.
 *
 * There are three such projections, not one, and they fail in different ways:
 *
 *  - [View.POSITIONS] can drift through a restore to the wrong point, a manual correction, a
 *    migration that touches one table, or a later change to the merge.
 *  - [View.POSITION_BOOK] is the in-memory mirror. Both the dashboard and the risk report are
 *    built from it rather than from the table, so a mirror that has drifted produces a wrong
 *    valuation, wrong Greeks and wrong VaR with nothing in the service to contradict them.
 *  - [View.LIMITS] never reads the position book by design — that independence is the whole point
 *    of running it in its own consumer group, and it is also why nothing else in the service
 *    would notice it disagreeing.
 */
data class Reconciliation(
    val checkedAtMillis: Long,
    val verdicts: List<ViewVerdict>,
) {
    /** True when every view that could be judged agreed. Inconclusive views do not make this false. */
    val agrees: Boolean get() = verdicts.none { it.divergences.isNotEmpty() }

    fun verdict(view: View): ViewVerdict? = verdicts.firstOrNull { it.view == view }

    companion object {
        /**
         * Judges every view against one ledger read.
         *
         * [ledger] carries its own consistency: the table totals beside it come from the same SQL
         * statement, so [View.POSITIONS] is compared at a single database instant and is never
         * inconclusive.
         *
         * The in-memory views in [inMemory] cannot share that instant, so they are compared only
         * when they sit at the ledger's high-water offset. Both null — nothing has traded, no view
         * has read anything — counts as equal, so a fresh deployment reports agreement rather than
         * spending its first interval unable to say anything.
         */
        fun of(
            ledger: LedgerSnapshot,
            inMemory: Map<View, ViewTotals>,
            checkedAtMillis: Long,
        ): Reconciliation {
            val ledgerQuantities = ledger.ledgerQuantities
            val verdicts =
                buildList {
                    add(
                        ViewVerdict(
                            view = View.POSITIONS,
                            symbolsChecked = ledger.totals.size,
                            divergences =
                                ledger.totals
                                    .filter { it.ledgerQuantity != it.positionQuantity }
                                    .map { Divergence(it.symbol, it.ledgerQuantity, it.positionQuantity) },
                        ),
                    )
                    for ((view, totals) in inMemory) {
                        add(judge(view, ledgerQuantities, ledger.highWaterOffset, totals))
                    }
                }
            return Reconciliation(checkedAtMillis, verdicts)
        }

        private fun judge(
            view: View,
            ledgerQuantities: Map<String, Long>,
            ledgerOffset: Long?,
            totals: ViewTotals,
        ): ViewVerdict {
            if (totals.offset != ledgerOffset) {
                return ViewVerdict(
                    view = view,
                    symbolsChecked = 0,
                    divergences = emptyList(),
                    inconclusive = Inconclusive(totals.offset, ledgerOffset),
                )
            }
            // Union of both sides, zero where a symbol is absent, matching the FULL OUTER JOIN the
            // table comparison uses. A view holding a symbol with no fills behind it and a ledger
            // the view never absorbed are the two most interesting divergences, not edge cases.
            val symbols = ledgerQuantities.keys + totals.quantities.keys
            return ViewVerdict(
                view = view,
                symbolsChecked = symbols.size,
                divergences =
                    symbols
                        .sorted()
                        .mapNotNull { symbol ->
                            val ledgerQuantity = ledgerQuantities[symbol] ?: 0
                            val viewQuantity = totals.quantities[symbol] ?: 0
                            if (ledgerQuantity == viewQuantity) null else Divergence(symbol, ledgerQuantity, viewQuantity)
                        },
            )
        }
    }
}
