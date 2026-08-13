package com.damianhoward.tradingsystem.position

import com.damianhoward.orderbook.model.Side
import com.damianhoward.tradingsystem.consume.Fill
import com.damianhoward.tradingsystem.consume.FillSource
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

    /**
     * Every symbol known to either table, with its signed ledger sum beside its stored position,
     * plus the ledger's high-water offset for [topic] — the whole input to the conservation check
     * in [Reconciliation], read at one instant. Symbols present in only one of the two are
     * included with zero on the missing side: a position with no fills behind it and a ledger the
     * position never absorbed are both divergences, and omitting either would hide one.
     */
    fun ledgerSnapshot(topic: String): LedgerSnapshot

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
                    statement.setLong(3, fill.timeMillis)
                    statement.setBigDecimal(4, fill.price)
                    statement.setLong(5, fill.timeMillis)
                    statement.setLong(6, fill.signedSize)
                    statement.setBigDecimal(7, fill.price)
                    statement.setLong(8, fill.timeMillis)
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

    override fun ledgerSnapshot(topic: String): LedgerSnapshot =
        connect().use { connection ->
            connection.prepareStatement(SELECT_LEDGER_SNAPSHOT).use { statement ->
                statement.setString(1, topic)
                statement.executeQuery().use { rows ->
                    var highWater: Long? = null
                    val totals =
                        buildList {
                            while (rows.next()) {
                                // Repeated on every row by the scalar subquery, and identical on
                                // all of them because one statement sees one SCN. Reading it here
                                // rather than in a second query is the point: the offset and the
                                // quantities it certifies have to describe the same instant.
                                val offset = rows.getLong("high_water_offset")
                                if (!rows.wasNull()) highWater = offset
                                add(
                                    SymbolTotals(
                                        symbol = rows.getString("symbol"),
                                        ledgerQuantity = rows.getLong("ledger_quantity"),
                                        positionQuantity = rows.getLong("position_quantity"),
                                    ),
                                )
                            }
                        }
                    LedgerSnapshot(highWater, totals)
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

        // quantity accumulates, so it is order-independent and adds unconditionally. last_price and
        // last_time_millis describe the most recent fill, which is an ordering claim, so they move
        // only when this fill is at least as recent as the row's.
        //
        // Today that guard cannot fire: orderbook.fills has one partition, so offsets totally order
        // the stream and fills arrive newest-last. It is written this way because the assumption is
        // invisible at the call site and repartitioning is an operational change nobody would
        // connect to a price going backwards. `>=` rather than `>` keeps the single-partition
        // behaviour exactly: two fills in the same millisecond still resolve to the later offset.
        private const val MERGE_POSITION =
            "MERGE INTO positions p USING (SELECT ? AS symbol FROM dual) src ON (p.symbol = src.symbol) " +
                "WHEN MATCHED THEN UPDATE SET quantity = p.quantity + ?, " +
                "last_price = CASE WHEN ? >= p.last_time_millis THEN ? ELSE p.last_price END, " +
                "last_time_millis = GREATEST(p.last_time_millis, ?) " +
                "WHEN NOT MATCHED THEN INSERT (symbol, quantity, last_price, last_time_millis) " +
                "VALUES (src.symbol, ?, ?, ?)"
        private const val SELECT_ONE =
            "SELECT symbol, quantity, last_price, last_time_millis FROM positions WHERE symbol = ?"
        private const val SELECT_ALL =
            "SELECT symbol, quantity, last_price, last_time_millis FROM positions ORDER BY symbol"
        private const val PING = "SELECT 1 FROM dual"

        // One statement, not two queries, because Oracle gives a single statement one read-consistent
        // snapshot: the aggregate and the positions it is compared against are read at the same SCN,
        // so a fill committing mid-check cannot produce a divergence that was never real.
        //
        // No source_topic predicate. record() merges the position for every fill it applies whatever
        // topic it arrived on, so the position is the sum over all topics; filtering to one here
        // would report a false divergence the day a second topic is consumed.
        //
        // FULL OUTER JOIN so a symbol in only one table still appears, with zero on the side that
        // lacks it — those are the two most interesting divergences, not edge cases to drop.
        //
        // The high-water offset IS topic-scoped, unlike the sums. It exists to say how much stream
        // the quantities above account for, and the in-memory views it is compared against each
        // track one topic; a max across topics would not correspond to any consumer's position.
        // It rides along as a scalar subquery rather than a second query so it shares the SCN —
        // an offset read a moment later would certify quantities it never saw.
        private const val SELECT_LEDGER_SNAPSHOT =
            "SELECT COALESCE(l.symbol, p.symbol) AS symbol, " +
                "COALESCE(l.ledger_quantity, 0) AS ledger_quantity, " +
                "COALESCE(p.quantity, 0) AS position_quantity, " +
                "(SELECT MAX(source_offset) FROM fills WHERE source_topic = ?) AS high_water_offset " +
                "FROM (SELECT symbol, SUM(signed_size) AS ledger_quantity FROM fills GROUP BY symbol) l " +
                "FULL OUTER JOIN positions p ON p.symbol = l.symbol " +
                "ORDER BY 1"
        private const val SELECT_LEDGER =
            "SELECT source_partition, source_offset, symbol, price, signed_size, maker_order_id, taker_order_id, " +
                "time_millis, exec_id FROM fills WHERE source_topic = ? ORDER BY source_partition, source_offset"
    }
}
