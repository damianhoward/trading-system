package com.damianhoward.tradingsystem.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class AppConfigTest {
    private val db = mapOf("DB_URL" to "jdbc:oracle:thin:@db", "DB_USER" to "app", "DB_PASSWORD" to "secret")

    @Test
    fun `defaults match the box-2 runbook`() {
        val config = AppConfig.fromEnv(db)
        assertEquals("127.0.0.1:9092", config.kafkaBootstrapServers)
        assertEquals("orderbook.fills", config.fillsTopic)
        assertEquals("orderbook.fills.DLT", config.deadLetterTopic)
        assertEquals(50, config.limitMaxPosition)
        assertEquals(BigDecimal("5000"), config.limitMaxNotional)
        assertEquals(8082, config.port)
    }

    @Test
    fun `every setting is overridable from the environment`() {
        val config =
            AppConfig.fromEnv(
                db +
                    mapOf(
                        "KAFKA_BOOTSTRAP_SERVERS" to "192.0.2.10:9094",
                        "FILLS_TOPIC" to "fills",
                        "FILLS_DLT_TOPIC" to "fills.dead",
                        "LIMIT_MAX_POSITION" to "100",
                        "LIMIT_MAX_NOTIONAL" to "12500.50",
                        "PORT" to "9000",
                    ),
            )
        assertEquals("192.0.2.10:9094", config.kafkaBootstrapServers)
        assertEquals("fills", config.fillsTopic)
        assertEquals("fills.dead", config.deadLetterTopic)
        assertEquals(100, config.limitMaxPosition)
        assertEquals(BigDecimal("12500.50"), config.limitMaxNotional)
        assertEquals(9000, config.port)
        assertEquals("jdbc:oracle:thin:@db", config.dbUrl)
    }

    @Test
    fun `limit ceilings must be positive numbers`() {
        listOf("LIMIT_MAX_POSITION", "LIMIT_MAX_NOTIONAL").forEach { name ->
            assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + (name to "many")) }
            assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + (name to "0")) }
            assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + (name to "-5")) }
        }
    }

    @Test
    fun `the database triple is required — a misconfigured process fails at startup`() {
        listOf("DB_URL", "DB_USER", "DB_PASSWORD").forEach { missing ->
            val e = assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db - missing) }
            assertEquals("$missing must be set", e.message)
        }
        assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + ("DB_URL" to " ")) }
    }

    @Test
    fun `a malformed port is rejected`() {
        assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + ("PORT" to "web")) }
        assertThrows(IllegalArgumentException::class.java) { AppConfig.fromEnv(db + ("PORT" to "70000")) }
    }
}
