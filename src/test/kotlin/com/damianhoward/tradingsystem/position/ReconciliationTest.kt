package com.damianhoward.tradingsystem.position

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ReconciliationTest {
    @Test
    fun `a book whose positions equal their ledger sums agrees`() {
        val result =
            Reconciliation.of(
                listOf(
                    SymbolTotals("SIM", ledgerQuantity = 12, positionQuantity = 12),
                    SymbolTotals("ACME", ledgerQuantity = -4, positionQuantity = -4),
                ),
                checkedAtMillis = 5_000,
            )

        assertTrue(result.agrees)
        assertEquals(2, result.symbolsChecked)
        assertEquals(5_000, result.checkedAtMillis)
    }

    @Test
    fun `an empty book agrees rather than failing for want of anything to check`() {
        val result = Reconciliation.of(emptyList(), checkedAtMillis = 1)

        assertTrue(result.agrees)
        assertEquals(0, result.symbolsChecked)
    }

    @Test
    fun `a position that does not equal its ledger sum diverges, carrying both quantities`() {
        val result =
            Reconciliation.of(
                listOf(SymbolTotals("SIM", ledgerQuantity = 10, positionQuantity = 13)),
                checkedAtMillis = 1,
            )

        assertFalse(result.agrees)
        val divergence = result.divergences.single()
        assertEquals("SIM", divergence.symbol)
        assertEquals(10, divergence.ledgerQuantity)
        assertEquals(13, divergence.positionQuantity)
    }

    @Test
    fun `the difference is signed from the position's side, so its direction names the fault`() {
        val overstated = Divergence("SIM", ledgerQuantity = 10, positionQuantity = 13)
        val understated = Divergence("SIM", ledgerQuantity = 10, positionQuantity = 6)

        assertEquals(3, overstated.difference)
        assertEquals(-4, understated.difference)
    }

    @Test
    fun `a symbol that reconciles is not reported alongside one that does not`() {
        val result =
            Reconciliation.of(
                listOf(
                    SymbolTotals("SIM", ledgerQuantity = 10, positionQuantity = 10),
                    SymbolTotals("ACME", ledgerQuantity = 7, positionQuantity = 9),
                ),
                checkedAtMillis = 1,
            )

        assertEquals(listOf("ACME"), result.divergences.map { it.symbol })
        assertEquals(2, result.symbolsChecked)
    }

    @Test
    fun `a flat position is only correct when its fills also net to zero`() {
        val netsToZero =
            Reconciliation.of(listOf(SymbolTotals("SIM", 0, 0)), checkedAtMillis = 1)
        val soldWithoutTheLedgerSayingSo =
            Reconciliation.of(listOf(SymbolTotals("SIM", ledgerQuantity = 5, positionQuantity = 0)), checkedAtMillis = 1)

        assertTrue(netsToZero.agrees)
        assertFalse(soldWithoutTheLedgerSayingSo.agrees)
    }
}
