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
 * A symbol whose stored position does not equal the fills it is derived from.
 *
 * Both quantities are carried rather than only the difference, because the two failures look
 * different in the numbers: a position that drifted from a correct ledger is a projection bug,
 * and a ledger short of the position is missing history.
 */
data class Divergence(
    val symbol: String,
    val ledgerQuantity: Long,
    val positionQuantity: Long,
) {
    /** Position minus ledger. Positive means the book claims more than its fills justify. */
    val difference: Long get() = positionQuantity - ledgerQuantity
}

/**
 * The result of one pass over the whole book, asserting the conservation property that every
 * position equals the signed sum of its ledger fills.
 *
 * The write path maintains that by construction — [PositionStore.record] inserts the fill and
 * moves the position in one transaction — which is exactly why it is worth checking. Nothing in
 * the service reads the ledger to serve a request, so a position that has drifted from it is
 * invisible: the dashboard, the limits view and the risk report all read `positions`, and all
 * three would agree with each other while disagreeing with the record of what actually traded.
 * The ways it can drift are real even though the write path is sound — a restore to the wrong
 * point, a manual correction, a migration that touches one table, or a later change to the merge.
 */
data class Reconciliation(
    val checkedAtMillis: Long,
    val symbolsChecked: Int,
    val divergences: List<Divergence>,
) {
    val agrees: Boolean get() = divergences.isEmpty()

    companion object {
        fun of(
            totals: List<SymbolTotals>,
            checkedAtMillis: Long,
        ): Reconciliation =
            Reconciliation(
                checkedAtMillis = checkedAtMillis,
                symbolsChecked = totals.size,
                divergences =
                    totals
                        .filter { it.ledgerQuantity != it.positionQuantity }
                        .map { Divergence(it.symbol, it.ledgerQuantity, it.positionQuantity) },
            )
    }
}
