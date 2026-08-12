package com.damianhoward.tradingsystem.consume

import com.damianhoward.orderbook.model.Side
import java.math.BigDecimal

/**
 * Where a record sits on the stream. The triple is unique per delivered record, which makes it
 * the natural idempotency key: applying the same coordinates twice is a replay, not a new fill.
 */
data class FillSource(
    val topic: String,
    val partition: Int,
    val offset: Long,
)

/**
 * One execution off the `orderbook.fills` topic, as orderbook's `KafkaMarketEgress` emits it:
 * `{"v":1,"execId":…,"symbol":…,"price":…,"size":…,"makerOrderId":…,"takerOrderId":…,"aggressor":…,"ts":…}`.
 * The [aggressor] is the taker's side ([Side.BID] lifted the offer, [Side.OFFER] hit the bid);
 * [price] arrives as a JSON string because the producer keeps it exact rather than a float.
 * [execId] is the producer's stable execution identity — carried in the payload so a copy of the
 * record at new stream coordinates is still the same execution; null on records published
 * before the id existed.
 */
data class Fill(
    val symbol: String,
    val price: BigDecimal,
    val size: Long,
    val makerOrderId: Long,
    val takerOrderId: Long,
    val aggressor: Side,
    val timeMillis: Long,
    val execId: String? = null,
) {
    init {
        require(price.signum() > 0) { "price must be positive, got $price" }
        require(size > 0) { "size must be positive, got $size" }
    }

    /** The taker's signed quantity: a BID aggressor bought [size], an OFFER aggressor sold it. */
    val signedSize: Long get() = if (aggressor == Side.BID) size else -size

    companion object {
        const val SCHEMA_VERSION = 1L

        /** @throws IllegalArgumentException on any deviation from the egress schema — the caller dead-letters, never retries */
        fun parse(json: String): Fill {
            val fields = FlatJson.parse(json)
            val version = fields.long("v")
            require(version == SCHEMA_VERSION) { "unsupported fill schema v$version" }
            return Fill(
                symbol = fields.string("symbol"),
                price = fields.decimal("price"),
                size = fields.long("size"),
                makerOrderId = fields.long("makerOrderId"),
                takerOrderId = fields.long("takerOrderId"),
                aggressor = side(fields.string("aggressor")),
                timeMillis = fields.long("ts"),
                execId = fields.stringOrNull("execId"),
            )
        }

        private fun side(name: String): Side =
            Side.entries.firstOrNull { it.name == name }
                ?: throw IllegalArgumentException("aggressor must be BID or OFFER, got '$name'")
    }
}
