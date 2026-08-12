package com.damianhoward.tradingsystem.consume

import java.math.BigDecimal

/**
 * The shape every orderbook egress record has: one flat JSON object of string and integer values,
 * no nesting, no arrays. Strict — anything outside that shape, a missing field, or a
 * wrongly-typed field throws [IllegalArgumentException], which the consumer maps to the
 * dead-letter topic rather than a retry (malformed input never heals).
 */
internal class FlatJson private constructor(
    private val fields: Map<String, Value>,
) {
    private data class Value(
        val text: String,
        val isString: Boolean,
    )

    fun string(name: String): String {
        val value = field(name)
        require(value.isString) { "$name must be a JSON string, got ${value.text}" }
        return value.text
    }

    /** The field as a JSON string, or null when absent — for additive optional fields. */
    fun stringOrNull(name: String): String? = if (name in fields) string(name) else null

    fun long(name: String): Long {
        val value = field(name)
        require(!value.isString) { "$name must be a JSON number, got a string" }
        return value.text.toLongOrNull() ?: throw IllegalArgumentException("$name is not an integer: ${value.text}")
    }

    /** A decimal carried as a JSON string — the egress emits prices this way to keep them exact. */
    fun decimal(name: String): BigDecimal =
        try {
            BigDecimal(string(name))
        } catch (_: NumberFormatException) {
            throw IllegalArgumentException("$name is not a decimal: ${string(name)}")
        }

    private fun field(name: String): Value = fields[name] ?: throw IllegalArgumentException("missing field: $name")

    companion object {
        fun parse(json: String): FlatJson {
            val parser = Parser(json)
            val fields = parser.readObject()
            parser.expectEnd()
            return FlatJson(fields)
        }
    }

    private class Parser(
        private val text: String,
    ) {
        private var at = 0

        fun readObject(): Map<String, Value> {
            expect('{')
            val fields = mutableMapOf<String, Value>()
            skipWhitespace()
            if (peek() == '}') {
                at++
                return fields
            }
            while (true) {
                val name = readString()
                expect(':')
                require(fields.put(name, readValue()) == null) { "duplicate field: $name" }
                skipWhitespace()
                when (val c = next()) {
                    ',' -> skipWhitespace()
                    '}' -> return fields
                    else -> fail("expected ',' or '}', got '$c'")
                }
            }
        }

        fun expectEnd() {
            skipWhitespace()
            require(at == text.length) { "trailing content after object: '${text.substring(at)}'" }
        }

        private fun readValue(): Value {
            skipWhitespace()
            return if (peek() == '"') {
                Value(readString(), isString = true)
            } else {
                val start = at
                while (at < text.length && text[at] !in ",}" && !text[at].isWhitespace()) at++
                val raw = text.substring(start, at)
                require(raw.isNotEmpty() && NUMBER.matches(raw)) { "unsupported value: '$raw'" }
                Value(raw, isString = false)
            }
        }

        // The egress escapes only backslash and quote; anything else after '\' is malformed.
        private fun readString(): String {
            skipWhitespace()
            expect('"')
            val out = StringBuilder()
            while (true) {
                when (val c = next()) {
                    '"' -> return out.toString()
                    '\\' ->
                        when (val escaped = next()) {
                            '"', '\\' -> out.append(escaped)
                            else -> fail("unsupported escape: '\\$escaped'")
                        }
                    else -> out.append(c)
                }
            }
        }

        private fun expect(c: Char) {
            skipWhitespace()
            if (next() != c) fail("expected '$c'")
        }

        private fun peek(): Char {
            if (at >= text.length) fail("unexpected end of input")
            return text[at]
        }

        private fun next(): Char = peek().also { at++ }

        private fun skipWhitespace() {
            while (at < text.length && text[at].isWhitespace()) at++
        }

        private fun fail(message: String): Nothing = throw IllegalArgumentException("$message at index $at")

        companion object {
            private val NUMBER = Regex("-?\\d+(\\.\\d+)?")
        }
    }
}
