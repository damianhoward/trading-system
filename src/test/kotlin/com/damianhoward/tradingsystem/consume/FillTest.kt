package com.damianhoward.tradingsystem.consume

import com.damianhoward.orderbook.model.Side
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class FillTest {
    // Exactly what KafkaMarketEgress.fillJson emits, 8-decimal price string included.
    private val egressJson =
        """{"v":1,"symbol":"SIM","price":"101.00000000","size":5,""" +
            """"makerOrderId":12,"takerOrderId":34,"aggressor":"BID","ts":1720620000000}"""

    @Test
    fun `parses the egress fill record`() {
        val fill = Fill.parse(egressJson)
        assertEquals(
            Fill(
                symbol = "SIM",
                price = BigDecimal("101.00000000"),
                size = 5,
                makerOrderId = 12,
                takerOrderId = 34,
                aggressor = Side.BID,
                timeMillis = 1720620000000,
            ),
            fill,
        )
    }

    @Test
    fun `carries the execution id when present and null when absent`() {
        val stamped = egressJson.replace(""""v":1,""", """"v":1,"execId":"1720620000000-7",""")
        assertEquals("1720620000000-7", Fill.parse(stamped).execId)
        assertEquals(null, Fill.parse(egressJson).execId, "pre-execId records still parse")
    }

    @Test
    fun `a BID aggressor bought, an OFFER aggressor sold`() {
        assertEquals(5L, Fill.parse(egressJson).signedSize)
        assertEquals(-5L, Fill.parse(egressJson.replace("BID", "OFFER")).signedSize)
    }

    @Test
    fun `rejects an unknown schema version`() {
        val e = assertThrows(IllegalArgumentException::class.java) { Fill.parse(egressJson.replace(""""v":1""", """"v":2""")) }
        assertEquals("unsupported fill schema v2", e.message)
    }

    @Test
    fun `rejects an unknown aggressor`() {
        assertThrows(IllegalArgumentException::class.java) { Fill.parse(egressJson.replace("BID", "BUY")) }
    }

    @Test
    fun `rejects a price that is not a decimal string`() {
        assertThrows(IllegalArgumentException::class.java) { Fill.parse(egressJson.replace(""""101.00000000"""", "101.0")) }
    }

    @Test
    fun `rejects non-positive price and size`() {
        assertThrows(IllegalArgumentException::class.java) { Fill.parse(egressJson.replace("101.00000000", "0")) }
        assertThrows(IllegalArgumentException::class.java) { Fill.parse(egressJson.replace(""""size":5""", """"size":0""")) }
    }

    @Test
    fun `rejects a record missing a field`() {
        assertThrows(IllegalArgumentException::class.java) { Fill.parse("""{"v":1,"symbol":"SIM"}""") }
    }
}
