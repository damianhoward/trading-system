package com.damianhoward.tradingsystem.consume

import com.damianhoward.tradingsystem.capture.FillHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration

/** What the consumer hands each raw record to; [RetryingHandler] is the production policy. */
fun interface RecordHandler {
    fun handle(record: ConsumerRecord<String, String>)
}

/** A valid fill still failing after the bounded retries; the process halts rather than skip it. */
class FillRetriesExhaustedException(
    source: FillSource,
    attempts: Int,
    cause: Exception,
) : RuntimeException(
        "fill handling still failing after $attempts attempts at " +
            "${source.topic}-${source.partition}@${source.offset}",
        cause,
    )

/**
 * The failure policy around fill processing, splitting poison from transient. A record that
 * fails to *parse* goes to the dead-letter topic and the stream continues — malformed input
 * never heals, so retrying it only stalls the stream. A record whose *handling* fails (the
 * database blinked) is retried up to [attempts] times, [backoff] apart, and on exhaustion the
 * process halts: a valid economic event must never be skippable by a transient outage, so
 * [FillRetriesExhaustedException] propagates, the consumer treats it as fatal, and systemd
 * restarts the process into a replay that idempotent application makes safe. The DLT is for
 * records that can never apply, not for records that could not apply *yet*.
 *
 * A [DeadLetterPublishException] propagates the same way: an unacknowledged dead-letter send
 * means the record is in neither stream, so the batch must not commit.
 *
 * [sleep] is injectable so tests drive the backoff without real waiting.
 */
class RetryingHandler(
    private val handler: FillHandler,
    private val deadLetters: DeadLetterPublisher,
    private val attempts: Int = DEFAULT_ATTEMPTS,
    private val backoff: Duration = DEFAULT_BACKOFF,
    private val sleep: (Duration) -> Unit = { Thread.sleep(it) },
) : RecordHandler {
    init {
        require(attempts >= 1) { "attempts must be at least 1, got $attempts" }
    }

    override fun handle(record: ConsumerRecord<String, String>) {
        val fill =
            try {
                Fill.parse(record.value())
            } catch (e: IllegalArgumentException) {
                deadLetters.publish(record, e)
                return
            }
        val source = FillSource(record.topic(), record.partition(), record.offset())
        var attempt = 1
        while (true) {
            try {
                handler.onFill(fill, source)
                return
            } catch (e: DeadLetterPublishException) {
                throw e
            } catch (e: Exception) {
                if (attempt == attempts) throw FillRetriesExhaustedException(source, attempts, e)
                attempt++
                sleep(backoff)
            }
        }
    }

    companion object {
        const val DEFAULT_ATTEMPTS = 3
        val DEFAULT_BACKOFF: Duration = Duration.ofMillis(500)
    }
}
