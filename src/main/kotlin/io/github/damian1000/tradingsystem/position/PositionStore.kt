package io.github.damian1000.tradingsystem.position

import io.github.damian1000.orderbook.model.Side
import io.github.damian1000.tradingsystem.consume.Fill
import io.github.damian1000.tradingsystem.consume.FillSource
import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLIntegrityConstraintViolationException

/** What [PositionStore.record] did with a fill. */
sealed interface RecordOutcome {
    /** The fill entered the ledger and [position] is the row the transaction committed. */
    data class Applied(
        val position: Position,
    ) : RecordOutcome

    /** The ledger already held these source coordinates — a replay; nothing changed. */
    data object Duplicate : RecordOutcome
}

/**
 * Durable home of the book of record: an append-only fill ledger plus the net position per
 * symbol, maintained together. The database outlives the process; the in-memory book warms from
 * it at startup.
 */
interface PositionStore {
    /**
     * Applies one fill transactionally: inserts the ledger row and moves the position by the
     * fill's signed size in the same transaction, so a crash can never persist one without the
     * other. Replayed coordinates (retry, redelivery, restart) return [RecordOutcome.Duplicate]
     * and change nothing.
     */
    fun record(
        fill: Fill,
        source: FillSource,
    ): RecordOutcome

    fun loadAll(): List<Position>

    /** Every ledger fill in stream order, plus the highest applied offset per partition of [topic]. */
    fun loadLedger(topic: String): Ledger

    /** True when the database answers a trivial query — the readiness probe's connectivity check. */
    fun ping(): Boolean
}

/** The replayable ledger for one topic: fills in (partition, offset) order and the high-water marks. */
data class Ledger(
    val fills: List<Fill>,
    val highWaterOffsets: Map<Int, Long>,
)

/**
 * Plain JDBC over the `fills` ledger and `positions` table (Flyway `V1`/`V2`), one connection
 * per operation — fills arrive at the live site's human rate, not a hot path, and a connection
 * that is opened, used and closed cannot go stale between fills. The position `MERGE` moves the
 * row by the fill's delta rather than overwriting it with a caller-supplied aggregate, so the
 * transaction is correct regardless of any in-memory state.
 */
class JdbcPositionStore(
    private val connect: () -> Connection,
) : PositionStore {
    override fun record(
        fill: Fill,
        source: FillSource,
    ): RecordOutcome =
        connect().use { connection ->
            connection.autoCommit = false
            // ADB warehouse services (high/medium) run DML in parallel by default, and a
            // parallel MERGE poisons its transaction for reads — the committed-row SELECT
            // below dies with ORA-12838. This transaction is OLTP-shaped, one ledger row and
            // one position row, so parallel DML has nothing to offer it; state that instead
            // of depending on the connection service's defaults. Must run before any DML.
            connection.createStatement().use { it.execute("ALTER SESSION DISABLE PARALLEL DML") }
            try {
                try {
                    connection.prepareStatement(INSERT_FILL).use { statement ->
                        statement.setString(1, source.topic)
                        statement.setInt(2, source.partition)
                        statement.setLong(3, source.offset)
                        statement.setString(4, fill.symbol)
                        statement.setBigDecimal(5, fill.price)
                        statement.setLong(6, fill.signedSize)
                        statement.setLong(7, fill.makerOrderId)
                        statement.setLong(8, fill.takerOrderId)
                        statement.setLong(9, fill.timeMillis)
                        statement.setString(10, fill.execId)
                        statement.executeUpdate()
                    }
                } catch (e: SQLIntegrityConstraintViolationException) {
                    // ORA-00001 specifically, not the whole exception type. Either uniqueness
                    // boundary means a replay: the coordinate primary key (same record again) or
                    // the exec_id index (same execution at new coordinates).
                    //
                    // Oracle raises this same exception class for NOT NULL and check-constraint
                    // failures, which are data errors and the opposite of a replay. Treating one
                    // as a duplicate would drop the fill, advance progress past it, and
                    // dead-letter nothing — silently losing a fill, which is the single outcome
                    // the surrounding design exists to prevent. Nothing can trigger that today
                    // (every column is NOT NULL and Kotlin's non-null types guard them, and there
                    // are no check constraints), so this is a latent hole rather than a live one.
                    // It is worth closing because the thing that opens it — adding a check
                    // constraint — is an ordinary schema change that would look entirely safe.
                    if (e.errorCode != ORA_UNIQUE_CONSTRAINT_VIOLATED) throw e
                    connection.rollback()
                    return RecordOutcome.Duplicate
                }
                connection.prepareStatement(MERGE_POSITION).use { statement ->
                    statement.setString(1, fill.symbol)
                    statement.setLong(2, fill.signedSize)
                    statement.setBigDecimal(3, fill.price)
                    statement.setLong(4, fill.timeMillis)
                    statement.setLong(5, fill.signedSize)
                    statement.setBigDecimal(6, fill.price)
                    statement.setLong(7, fill.timeMillis)
                    statement.executeUpdate()
                }
                val position = selectPosition(connection, fill.symbol)
                connection.commit()
                RecordOutcome.Applied(position)
            } catch (e: Exception) {
                connection.rollback()
                throw e
            }
        }

    private fun selectPosition(
        connection: Connection,
        symbol: String,
    ): Position =
        connection.prepareStatement(SELECT_ONE).use { statement ->
            statement.setString(1, symbol)
            statement.executeQuery().use { rows ->
                check(rows.next()) { "position row missing for $symbol after merge" }
                position(rows)
            }
        }

    override fun loadAll(): List<Position> =
        connect().use { connection ->
            connection.prepareStatement(SELECT_ALL).use { statement ->
                statement.executeQuery().use { rows ->
                    buildList { while (rows.next()) add(position(rows)) }
                }
            }
        }

    override fun loadLedger(topic: String): Ledger =
        connect().use { connection ->
            connection.prepareStatement(SELECT_LEDGER).use { statement ->
                statement.setString(1, topic)
                statement.executeQuery().use { rows ->
                    val fills = mutableListOf<Fill>()
                    val highWater = mutableMapOf<Int, Long>()
                    while (rows.next()) {
                        val signed = rows.getLong("signed_size")
                        fills.add(
                            Fill(
                                symbol = rows.getString("symbol"),
                                price = rows.getBigDecimal("price"),
                                size = if (signed < 0) -signed else signed,
                                makerOrderId = rows.getLong("maker_order_id"),
                                takerOrderId = rows.getLong("taker_order_id"),
                                aggressor = if (signed < 0) Side.OFFER else Side.BID,
                                timeMillis = rows.getLong("time_millis"),
                                execId = rows.getString("exec_id"),
                            ),
                        )
                        highWater[rows.getInt("source_partition")] = rows.getLong("source_offset")
                    }
                    Ledger(fills, highWater)
                }
            }
        }

    override fun ping(): Boolean =
        try {
            connect().use { connection ->
                connection.prepareStatement(PING).use { statement ->
                    statement.executeQuery().use { rows -> rows.next() }
                }
            }
        } catch (_: Exception) {
            false
        }

    private fun position(rows: ResultSet): Position =
        Position(
            symbol = rows.getString("symbol"),
            quantity = rows.getLong("quantity"),
            lastPrice = rows.getBigDecimal("last_price"),
            lastTimeMillis = rows.getLong("last_time_millis"),
        )

    companion object {
        /** ORA-00001, "unique constraint violated" — the only integrity failure that means replay. */
        private const val ORA_UNIQUE_CONSTRAINT_VIOLATED = 1

        private const val INSERT_FILL =
            "INSERT INTO fills (source_topic, source_partition, source_offset, symbol, price, signed_size, " +
                "maker_order_id, taker_order_id, time_millis, exec_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        private const val MERGE_POSITION =
            "MERGE INTO positions p USING (SELECT ? AS symbol FROM dual) src ON (p.symbol = src.symbol) " +
                "WHEN MATCHED THEN UPDATE SET quantity = p.quantity + ?, last_price = ?, last_time_millis = ? " +
                "WHEN NOT MATCHED THEN INSERT (symbol, quantity, last_price, last_time_millis) " +
                "VALUES (src.symbol, ?, ?, ?)"
        private const val SELECT_ONE =
            "SELECT symbol, quantity, last_price, last_time_millis FROM positions WHERE symbol = ?"
        private const val SELECT_ALL =
            "SELECT symbol, quantity, last_price, last_time_millis FROM positions ORDER BY symbol"
        private const val PING = "SELECT 1 FROM dual"
        private const val SELECT_LEDGER =
            "SELECT source_partition, source_offset, symbol, price, signed_size, maker_order_id, taker_order_id, " +
                "time_millis, exec_id FROM fills WHERE source_topic = ? ORDER BY source_partition, source_offset"
    }
}
