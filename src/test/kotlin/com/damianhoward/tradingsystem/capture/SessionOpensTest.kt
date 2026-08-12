package com.damianhoward.tradingsystem.capture

import com.damianhoward.orderbook.model.Side
import com.damianhoward.tradingsystem.consume.Fill
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class SessionOpensTest {
    private val day = 86_400_000L

    private fun fill(
        symbol: String,
        price: String,
        ts: Long,
    ) = Fill(symbol, BigDecimal(price), 1, 1, 2, Side.BID, ts)

    @Test
    fun `the first fill of a day sets the open and later same-day fills keep it`() {
        val opens = SessionOpens()
        opens.observe(fill("SIM", "100.00", ts = 10 * day + 1_000))
        opens.observe(fill("SIM", "105.00", ts = 10 * day + 2_000))

        assertEquals(BigDecimal("100.00"), opens.openFor("SIM"))
    }

    @Test
    fun `a fill on a later UTC day rolls the open`() {
        val opens = SessionOpens()
        opens.observe(fill("SIM", "100.00", ts = 10 * day + 1_000))
        opens.observe(fill("SIM", "107.00", ts = 11 * day + 500))

        assertEquals(BigDecimal("107.00"), opens.openFor("SIM"))
    }

    @Test
    fun `symbols keep independent opens`() {
        val opens = SessionOpens()
        opens.observe(fill("AAPL", "300.00", ts = 10 * day))
        opens.observe(fill("SIM", "100.00", ts = 10 * day + 1))

        assertEquals(BigDecimal("300.00"), opens.openFor("AAPL"))
        assertEquals(BigDecimal("100.00"), opens.openFor("SIM"))
        assertNull(opens.openFor("SPCX"), "no fill, no open")
    }

    @Test
    fun `replaying the same fills reproduces the same opens — restart safety`() {
        val fills =
            listOf(
                fill("SIM", "100.00", ts = 10 * day + 1_000),
                fill("SIM", "105.00", ts = 10 * day + 2_000),
                fill("AAPL", "300.00", ts = 11 * day),
            )
        val live = SessionOpens().apply { fills.forEach(::observe) }
        val warmed = SessionOpens().apply { fills.forEach(::observe) }

        assertEquals(live.openFor("SIM"), warmed.openFor("SIM"))
        assertEquals(live.openFor("AAPL"), warmed.openFor("AAPL"))
    }
}
