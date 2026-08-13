package com.damianhoward.tradingsystem.position

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ReconciliationTest {
    private fun ledger(
        vararg totals: SymbolTotals,
        offset: Long? = 4L,
    ) = LedgerSnapshot(offset, totals.toList())

    private fun of(
        ledger: LedgerSnapshot,
        inMemory: Map<View, ViewTotals> = emptyMap(),
        checkedAtMillis: Long = 1,
    ) = Reconciliation.of(ledger, inMemory, checkedAtMillis)

    private fun positions(result: Reconciliation) = result.verdict(View.POSITIONS)!!

    @Test
    fun `a book whose positions equal their ledger sums agrees`() {
        val result =
            of(
                ledger(
                    SymbolTotals("SIM", ledgerQuantity = 12, positionQuantity = 12),
                    SymbolTotals("ACME", ledgerQuantity = -4, positionQuantity = -4),
                ),
                checkedAtMillis = 5_000,
            )

        assertTrue(result.agrees)
        assertEquals(2, positions(result).symbolsChecked)
        assertEquals(5_000, result.checkedAtMillis)
    }

    @Test
    fun `an empty book agrees rather than failing for want of anything to check`() {
        val result = of(ledger(offset = null))

        assertTrue(result.agrees)
        assertEquals(0, positions(result).symbolsChecked)
    }

    @Test
    fun `a position that does not equal its ledger sum diverges, carrying both quantities`() {
        val result = of(ledger(SymbolTotals("SIM", ledgerQuantity = 10, positionQuantity = 13)))

        assertFalse(result.agrees)
        val divergence = positions(result).divergences.single()
        assertEquals("SIM", divergence.symbol)
        assertEquals(10, divergence.ledgerQuantity)
        assertEquals(13, divergence.viewQuantity)
    }

    @Test
    fun `the difference is signed from the view's side, so its direction names the fault`() {
        val overstated = Divergence("SIM", ledgerQuantity = 10, viewQuantity = 13)
        val understated = Divergence("SIM", ledgerQuantity = 10, viewQuantity = 6)

        assertEquals(3, overstated.difference)
        assertEquals(-4, understated.difference)
    }

    @Test
    fun `a symbol that reconciles is not reported alongside one that does not`() {
        val result =
            of(
                ledger(
                    SymbolTotals("SIM", ledgerQuantity = 10, positionQuantity = 10),
                    SymbolTotals("ACME", ledgerQuantity = 7, positionQuantity = 9),
                ),
            )

        assertEquals(listOf("ACME"), positions(result).divergences.map { it.symbol })
        assertEquals(2, positions(result).symbolsChecked)
    }

    @Test
    fun `a flat position is only correct when its fills also net to zero`() {
        val netsToZero = of(ledger(SymbolTotals("SIM", 0, 0)))
        val soldWithoutTheLedgerSayingSo =
            of(ledger(SymbolTotals("SIM", ledgerQuantity = 5, positionQuantity = 0)))

        assertTrue(netsToZero.agrees)
        assertFalse(soldWithoutTheLedgerSayingSo.agrees)
    }

    @Test
    fun `the table is judged from the database's own snapshot, so it is never inconclusive`() {
        // It arrives beside the ledger sum from one statement at one SCN. There is no offset to
        // compare and nothing to be out of step with.
        val result = of(ledger(SymbolTotals("SIM", 4, 4), offset = 99))

        assertNull(positions(result).inconclusive)
    }

    @Test
    fun `an in-memory view at the ledger's offset is judged against it`() {
        val result =
            of(
                ledger(SymbolTotals("SIM", 10, 10), offset = 4),
                mapOf(View.LIMITS to ViewTotals(4, mapOf("SIM" to 10L))),
            )

        val limits = result.verdict(View.LIMITS)!!
        assertTrue(limits.agrees)
        assertNull(limits.inconclusive)
    }

    @Test
    fun `an in-memory view at a different offset is inconclusive rather than divergent`() {
        val result =
            of(
                ledger(SymbolTotals("SIM", 10, 10), offset = 4),
                mapOf(View.LIMITS to ViewTotals(2, mapOf("SIM" to 6L))),
            )

        val limits = result.verdict(View.LIMITS)!!
        assertFalse(limits.agrees, "unjudged is not agreement")
        assertTrue(limits.divergences.isEmpty(), "and it is not divergence either")
        assertEquals(Inconclusive(viewOffset = 2, ledgerOffset = 4), limits.inconclusive)
        assertTrue(result.agrees, "the pass as a whole still holds: nothing was shown to be wrong")
    }

    @Test
    fun `a view ahead of the ledger is inconclusive too, which is what a duplicate at new coordinates leaves`() {
        // An exec_id already in the ledger arriving at a fresh offset advances the consumer's
        // progress without inserting a row, so a view can legitimately sit past the high-water
        // mark. Treating that as a divergence would fail the probe over a working replay guard.
        val result =
            of(
                ledger(SymbolTotals("SIM", 10, 10), offset = 4),
                mapOf(View.POSITION_BOOK to ViewTotals(5, mapOf("SIM" to 10L))),
            )

        assertNotNull(result.verdict(View.POSITION_BOOK)!!.inconclusive)
    }

    @Test
    fun `nothing traded and nothing read counts as agreement, not as a view that cannot be judged`() {
        // Otherwise every fresh deployment would spend its first passes unable to say anything.
        val result =
            of(
                ledger(offset = null),
                mapOf(View.LIMITS to ViewTotals(null, emptyMap())),
            )

        assertTrue(result.verdict(View.LIMITS)!!.agrees)
    }

    @Test
    fun `a symbol only one side knows about is a divergence, not a symbol to drop`() {
        val ledgerOnly =
            of(
                ledger(SymbolTotals("SIM", 10, 10), offset = 4),
                mapOf(View.LIMITS to ViewTotals(4, emptyMap())),
            )
        val viewOnly =
            of(
                ledger(offset = 4),
                mapOf(View.LIMITS to ViewTotals(4, mapOf("GHOST" to 3L))),
            )

        assertEquals(
            Divergence("SIM", ledgerQuantity = 10, viewQuantity = 0),
            ledgerOnly.verdict(View.LIMITS)!!.divergences.single(),
        )
        assertEquals(
            Divergence("GHOST", ledgerQuantity = 0, viewQuantity = 3),
            viewOnly.verdict(View.LIMITS)!!.divergences.single(),
        )
    }

    @Test
    fun `each view is judged on its own, so one drifting does not implicate the others`() {
        val result =
            of(
                ledger(SymbolTotals("SIM", 10, 10), offset = 4),
                mapOf(
                    View.POSITION_BOOK to ViewTotals(4, mapOf("SIM" to 10L)),
                    View.LIMITS to ViewTotals(4, mapOf("SIM" to 3L)),
                ),
            )

        assertFalse(result.agrees)
        assertTrue(result.verdict(View.POSITIONS)!!.agrees)
        assertTrue(result.verdict(View.POSITION_BOOK)!!.agrees)
        assertEquals(listOf("SIM"), result.verdict(View.LIMITS)!!.divergences.map { it.symbol })
    }

    @Test
    fun `the view labels are stable, because a metrics label set is a contract`() {
        // Renaming one silently splits a series in the collector: the old name stops and a new one
        // starts, with no error anywhere and no way to query across the break.
        assertEquals(
            listOf("positions", "position_book", "limits"),
            View.entries.map { it.label },
        )
    }
}
