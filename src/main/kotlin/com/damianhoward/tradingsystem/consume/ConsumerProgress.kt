package com.damianhoward.tradingsystem.consume

/**
 * How far a consumer path has read: the last processed record's [offset] and the fill's own
 * [fillTimeMillis]. The dashboard compares the two paths' progress to show whether the
 * positions and exposure views describe the same point on the stream.
 */
data class ConsumerProgress(
    val offset: Long,
    val fillTimeMillis: Long,
)
