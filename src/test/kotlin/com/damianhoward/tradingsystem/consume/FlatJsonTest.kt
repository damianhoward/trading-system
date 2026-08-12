package com.damianhoward.tradingsystem.consume

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class FlatJsonTest {
    @Test
    fun `reads string, integer and decimal fields from a flat object`() {
        val json = FlatJson.parse("""{"symbol":"SIM","size":5,"price":"101.25","ts":1720620000000}""")
        assertEquals("SIM", json.string("symbol"))
        assertEquals(5L, json.long("size"))
        assertEquals(BigDecimal("101.25"), json.decimal("price"))
        assertEquals(1720620000000L, json.long("ts"))
    }

    @Test
    fun `unescapes quotes and backslashes in strings`() {
        val json = FlatJson.parse("""{"symbol":"A\"B\\C"}""")
        assertEquals("A\"B\\C", json.string("symbol"))
    }

    @Test
    fun `tolerates whitespace between tokens`() {
        val json = FlatJson.parse("""  { "a" : 1 , "b" : "x" }  """)
        assertEquals(1L, json.long("a"))
        assertEquals("x", json.string("b"))
    }

    @Test
    fun `accepts an empty object`() {
        val json = FlatJson.parse("{}")
        assertThrows(IllegalArgumentException::class.java) { json.string("anything") }
    }

    @Test
    fun `negative numbers parse`() {
        assertEquals(-3L, FlatJson.parse("""{"n":-3}""").long("n"))
    }

    @Test
    fun `a missing field names itself in the error`() {
        val e = assertThrows(IllegalArgumentException::class.java) { FlatJson.parse("""{"a":1}""").long("b") }
        assertEquals("missing field: b", e.message)
    }

    @Test
    fun `a string where a number is expected is rejected`() {
        assertThrows(IllegalArgumentException::class.java) { FlatJson.parse("""{"a":"1"}""").long("a") }
    }

    @Test
    fun `a number where a string is expected is rejected`() {
        assertThrows(IllegalArgumentException::class.java) { FlatJson.parse("""{"a":1}""").string("a") }
    }

    @Test
    fun `a non-decimal string field is rejected as a decimal`() {
        assertThrows(IllegalArgumentException::class.java) { FlatJson.parse("""{"a":"abc"}""").decimal("a") }
    }

    @Test
    fun `malformed input is rejected`() {
        listOf(
            "",
            "[1,2]",
            """{"a":1""",
            """{"a":}""",
            """{"a":true}""",
            """{"a":{"nested":1}}""",
            """{"a":1}trailing""",
            """{"a":1,"a":2}""",
            """{"a":"bad \n escape"}""", // raw string: a literal backslash-n, an unsupported escape
            """{"a":"unterminated""",
            """{a:1}""",
        ).forEach { json ->
            assertThrows(IllegalArgumentException::class.java, { FlatJson.parse(json) }, "should reject: $json")
        }
    }
}
