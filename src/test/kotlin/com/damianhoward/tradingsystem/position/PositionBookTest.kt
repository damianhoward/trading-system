package com.damianhoward.tradingsystem.position

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class PositionBookTest {
    private fun position(
        symbol: String = "SIM",
        quantity: Long = 5,
        price: String = "101.00",
        ts: Long = 1000,
    ) = Position(symbol, quantity, BigDecimal(price), ts)

    @Test
    fun `put replaces the symbol's row with the store's committed truth`() {
        val book = PositionBook()
        book.put(position(quantity = 5))
        book.put(position(quantity = 8, price = "102.00", ts = 2000))
        assertEquals(Position("SIM", 8, BigDecimal("102.00"), 2000), book.positionOf("SIM"))
    }

    @Test
    fun `a flattened symbol stays on the book at zero`() {
        val book = PositionBook()
        book.put(position(quantity = 5))
        book.put(position(quantity = 0, ts = 2000))
        assertEquals(0, book.positionOf("SIM")?.quantity)
        assertEquals(1, book.all().size, "a symbol that traded stays visible when flat")
    }

    @Test
    fun `positions are tracked per symbol and listed in symbol order`() {
        val book = PositionBook()
        book.put(position(symbol = "ZZZ", quantity = 5))
        book.put(position(symbol = "AAA", quantity = -2))
        assertEquals(listOf("AAA", "ZZZ"), book.all().map { it.symbol })
        assertEquals(-2, book.positionOf("AAA")?.quantity)
        assertEquals(5, book.positionOf("ZZZ")?.quantity)
        assertNull(book.positionOf("MISSING"))
    }

    @Test
    fun `restore replaces in-memory state with the persisted book`() {
        val book = PositionBook()
        book.put(position(symbol = "OLD"))
        val persisted = Position("SIM", 7, BigDecimal("99.50"), 500)
        book.restore(listOf(persisted))
        assertEquals(listOf(persisted), book.all())
        assertNull(book.positionOf("OLD"))
    }
}
