package com.damianhoward.tradingsystem.config

import java.math.BigDecimal

/**
 * Process configuration, read once from the environment at startup. Kafka, limits, and web
 * settings have defaults that match the box-2 runbook (broker on localhost, orderbook's topic
 * names, ceilings sized to the live market); the database triple is required — Slice 1 is
 * DB-backed by design, so a misconfigured process fails at startup, not on the first fill.
 */
data class AppConfig(
    val kafkaBootstrapServers: String,
    val fillsTopic: String,
    val deadLetterTopic: String,
    val limitMaxPosition: Long,
    val limitMaxNotional: BigDecimal,
    val dbUrl: String,
    val dbUser: String,
    val dbPassword: String,
    val port: Int,
) {
    companion object {
        fun fromEnv(env: Map<String, String>): AppConfig =
            AppConfig(
                kafkaBootstrapServers = env["KAFKA_BOOTSTRAP_SERVERS"] ?: "127.0.0.1:9092",
                fillsTopic = env["FILLS_TOPIC"] ?: "orderbook.fills",
                deadLetterTopic = env["FILLS_DLT_TOPIC"] ?: "orderbook.fills.DLT",
                limitMaxPosition = positiveLong(env["LIMIT_MAX_POSITION"] ?: "50", "LIMIT_MAX_POSITION"),
                limitMaxNotional = positiveDecimal(env["LIMIT_MAX_NOTIONAL"] ?: "5000", "LIMIT_MAX_NOTIONAL"),
                dbUrl = required(env, "DB_URL"),
                dbUser = required(env, "DB_USER"),
                dbPassword = required(env, "DB_PASSWORD"),
                port = port(env["PORT"] ?: "8082"),
            )

        private fun required(
            env: Map<String, String>,
            name: String,
        ): String = env[name]?.takeUnless { it.isBlank() } ?: throw IllegalArgumentException("$name must be set")

        private fun port(raw: String): Int {
            val port = raw.toIntOrNull() ?: throw IllegalArgumentException("PORT is not a number: '$raw'")
            require(port in 0..65535) { "PORT out of range: $port" }
            return port
        }

        private fun positiveLong(
            raw: String,
            name: String,
        ): Long {
            val value = raw.toLongOrNull() ?: throw IllegalArgumentException("$name is not a number: '$raw'")
            require(value > 0) { "$name must be positive: $value" }
            return value
        }

        private fun positiveDecimal(
            raw: String,
            name: String,
        ): BigDecimal {
            val value = raw.toBigDecimalOrNull() ?: throw IllegalArgumentException("$name is not a number: '$raw'")
            require(value.signum() > 0) { "$name must be positive: $value" }
            return value
        }
    }
}
