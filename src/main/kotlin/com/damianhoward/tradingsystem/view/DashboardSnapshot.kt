package com.damianhoward.tradingsystem.view

import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.exposure.ExposureReport
import com.damianhoward.tradingsystem.position.Position
import com.damianhoward.tradingsystem.pricing.BookRisk

/**
 * Everything the dashboard renders, at one moment: the booked [positions], the [book] risk —
 * one report per position plus the sums that are honest to sum (null before anything has
 * traded) — the [exposure] view the detector derives, and where each consumer path sits on the
 * stream. The two paths are independent consumers, so the `sync` block states whether they
 * describe the same stream position instead of leaving the reader to assume it, along with the
 * poison records dead-lettered this session ([deadLetters]). [toJson] is the wire contract
 * `/api/state` and the SSE stream both carry; `v:2` replaced the single-instrument `report`
 * with the whole-book shape.
 */
data class DashboardSnapshot(
    val positions: List<Position>,
    val book: BookRisk?,
    val exposure: ExposureReport,
    val positionsProgress: ConsumerProgress? = null,
    val duplicates: Long = 0,
    val deadLetters: Long = 0,
) {
    /** True when both paths have read to the same offset (or neither has seen a fill). */
    val coherent: Boolean get() = positionsProgress?.offset == exposure.progress?.offset

    fun toJson(): String =
        """{"v":2,"positions":[${positions.joinToString(",", transform = ::positionJson)}],""" +
            """"book":${book?.toJson() ?: "null"},""" +
            """"exposure":${exposure.toJson()},""" +
            """"sync":{"positions":${progressJson(positionsProgress)},"exposure":${progressJson(exposure.progress)},""" +
            """"coherent":$coherent,"duplicatesDropped":$duplicates,"deadLetters":$deadLetters}}"""

    private fun progressJson(p: ConsumerProgress?): String =
        if (p == null) "null" else """{"offset":${p.offset},"fillTs":${p.fillTimeMillis}}"""

    // Prices are exact JSON numbers via their plain-string form, matching RiskReport's convention.
    private fun positionJson(position: Position): String =
        """{"symbol":${quote(position.symbol)},"quantity":${position.quantity},""" +
            """"lastPrice":${position.lastPrice.toPlainString()},"lastTimeMillis":${position.lastTimeMillis}}"""

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
