package io.github.damian1000.tradingsystem.position

import io.github.damian1000.orderbook.model.Side
import io.github.damian1000.tradingsystem.consume.Fill
import io.github.damian1000.tradingsystem.consume.FillSource
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.oracle.OracleContainer
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException

/**
 * The store against a real Oracle, schema applied by the same Flyway migrations production runs —
 * an in-memory stand-in can't fail on Oracle's types, MERGE semantics, ORA-00001, or the
 * migrations themselves. Skips only where Docker is absent; always runs in CI.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcPositionStoreTest {
    companion object {
        // Three minutes, not the default: on a loaded workstation the database's cold start
        // brushes the shorter window and fails the suite before a single test runs.
        @Container
        @JvmField
        val oracle: OracleContainer =
            OracleContainer("gvenzl/oracle-free:23-slim-faststart")
                .withStartupTimeout(java.time.Duration.ofMinutes(3))
    }

    private val store = JdbcPositionStore { DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password) }

    @BeforeEach
    fun cleanSchema() {
        Flyway
            .configure()
            .dataSource(oracle.jdbcUrl, oracle.username, oracle.password)
            .cleanDisabled(false)
            .load()
            .apply { clean() }
            .migrate()
    }

    private fun fill(
        symbol: String = "SIM",
        side: Side = Side.BID,
        size: Long = 5,
        price: String = "101.00000000",
        ts: Long = 1000,
    ) = Fill(symbol, BigDecimal(price), size, 11, 22, side, ts)

    private fun source(offset: Long) = FillSource("orderbook.fills", 0, offset)

    @Test
    fun `an empty table loads an empty book`() {
        assertTrue(store.loadAll().isEmpty())
        assertTrue(store.loadLedger("orderbook.fills").fills.isEmpty())
    }

    @Test
    fun `fills accumulate a net position through the ledger`() {
        store.record(fill(size = 5), source(1))
        val outcome = store.record(fill(side = Side.OFFER, size = 8, price = "102.00000000", ts = 2000), source(2))

        val position = (outcome as RecordOutcome.Applied).position
        assertEquals(-3, position.quantity)
        assertEquals(0, BigDecimal("102.00000000").compareTo(position.lastPrice))
        assertEquals(2000, position.lastTimeMillis)
        assertEquals(position.quantity, store.loadAll().single().quantity, "the returned row is the committed row")
    }

    @Test
    fun `the same execution at new coordinates is a duplicate — republication cannot double-count`() {
        store.record(fill(size = 5).copy(execId = "42-1"), source(1))
        val copy = store.record(fill(size = 5).copy(execId = "42-1"), FillSource("orderbook.fills.replayed", 0, 0))

        assertEquals(RecordOutcome.Duplicate, copy)
        assertEquals(5, store.loadAll().single().quantity, "one execution, one application")
        assertEquals(
            "42-1",
            store
                .loadLedger("orderbook.fills")
                .fills
                .single()
                .execId,
            "identity survives the ledger round-trip",
        )
    }

    @Test
    fun `records without execution ids dedupe by coordinates alone — nulls never collide`() {
        store.record(fill(size = 5), source(1))
        val second = store.record(fill(size = 3, ts = 2000), source(2))

        assertTrue(second is RecordOutcome.Applied, "two null exec_ids must not trip the unique index")
        assertEquals(8, store.loadAll().single().quantity)
    }

    @Test
    fun `replayed source coordinates are dropped — retry, redelivery, and restart cannot double-apply`() {
        store.record(fill(size = 5), source(1))
        val replay = store.record(fill(size = 5), source(1))

        assertEquals(RecordOutcome.Duplicate, replay)
        assertEquals(5, store.loadAll().single().quantity, "the position is unchanged by the replay")
        assertEquals(1, store.loadLedger("orderbook.fills").fills.size, "the ledger keeps one row per record")
    }

    @Test
    fun `a check-constraint violation is a failure, not a replay`() {
        // The duplicate path keys on ORA-00001 rather than on the exception class, and this is
        // why. Oracle raises SQLIntegrityConstraintViolationException for a check constraint too,
        // so classifying by type would call this a replay: the fill would be dropped, progress
        // would advance past it, and nothing would dead-letter it — a silently lost fill, which
        // is the one outcome the surrounding design exists to prevent.
        //
        // Nothing in the live schema can raise ORA-02290 today, so the constraint is added here.
        // The hole is latent, and what would open it is an ordinary schema change.
        DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password).use { connection ->
            connection.createStatement().use {
                it.execute("ALTER TABLE fills ADD CONSTRAINT fills_price_floor CHECK (price > 1000)")
            }
        }

        val failure = assertThrows(SQLException::class.java) { store.record(fill(price = "101.00000000"), source(1)) }

        assertEquals(2290, failure.errorCode, "ORA-02290, check constraint violated — propagated rather than swallowed")
        assertTrue(store.loadLedger("orderbook.fills").fills.isEmpty(), "the rejected fill left nothing behind")
        assertTrue(store.loadAll().isEmpty())
    }

    @Test
    fun `a failure after the ledger insert rolls the whole transaction back`() {
        val failing =
            JdbcPositionStore {
                FailOnMerge(DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password))
            }

        assertThrows(SQLException::class.java) { failing.record(fill(size = 5), source(1)) }

        assertTrue(store.loadLedger("orderbook.fills").fills.isEmpty(), "the ledger row must not survive alone")
        assertTrue(store.loadAll().isEmpty())
        // The coordinates are still free: the consumer's retry applies cleanly.
        assertTrue(store.record(fill(size = 5), source(1)) is RecordOutcome.Applied)
        assertEquals(5, store.loadAll().single().quantity)
    }

    @Test
    fun `the ledger replays in stream order with high-water marks and faithful fills`() {
        store.record(fill(size = 5, ts = 1000), source(1))
        store.record(fill(side = Side.OFFER, size = 2, ts = 2000), source(2))
        store.record(fill(symbol = "ABC", size = 1, ts = 3000), source(3))

        val ledger = store.loadLedger("orderbook.fills")
        assertEquals(3, ledger.fills.size)
        assertEquals(mapOf(0 to 3L), ledger.highWaterOffsets)
        val second = ledger.fills[1]
        assertEquals(Side.OFFER, second.aggressor)
        assertEquals(-2, second.signedSize)
        assertEquals(11, second.makerOrderId)
        assertEquals(22, second.takerOrderId)
        assertTrue(ledger.fills.map { it.timeMillis } == listOf(1000L, 2000L, 3000L), "stream order, not insertion luck")
    }

    @Test
    fun `saves and reloads positions, preserving the 8-decimal price exactly`() {
        store.record(fill(symbol = "SIM", side = Side.OFFER, size = 42, price = "101.00000000", ts = 1720620000000), source(1))
        store.record(fill(symbol = "ABC", size = 7, price = "0.00000001", ts = 1), source(2))

        val loaded = store.loadAll()
        assertEquals(listOf("ABC", "SIM"), loaded.map { it.symbol }, "loadAll orders by symbol")
        assertEquals(0, BigDecimal("0.00000001").compareTo(loaded[0].lastPrice))
        assertEquals(-42, loaded[1].quantity)
    }

    @Test
    fun `ping answers true against a live database and false against a dead one`() {
        assertTrue(store.ping())
        val dead = JdbcPositionStore { throw SQLException("no route to database") }
        assertFalse(dead.ping())
    }

    /** A connection that fails exactly between the ledger insert and the position merge. */
    private class FailOnMerge(
        private val real: Connection,
    ) : Connection by real {
        override fun prepareStatement(sql: String): PreparedStatement =
            if (sql.startsWith("MERGE")) throw SQLException("simulated failure mid-transaction") else real.prepareStatement(sql)
    }
}
