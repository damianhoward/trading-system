package com.damianhoward.tradingsystem.position

import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.Connection
import java.sql.DriverManager

/**
 * Migrating onto the schema production was already running, with rows already in it.
 *
 * Every other test here reaches the current schema by `clean()` then `migrate()`, which only ever
 * exercises empty → latest. That is not the upgrade production performs. A deploy applies the new
 * migration to a database holding the previous version *and its data*, and the failures that shape
 * belongs to — a constraint an existing row violates, a `NOT NULL` added with no default, an index
 * that collides on rows already present — are invisible to a migration run against nothing.
 *
 * The check that earns this class is V4's. It adds a nullable `exec_id` and a unique index over it,
 * and the migration's own comment claims Oracle's unique index ignores entirely-null entries. On an
 * empty database that claim is never tested: there are no null rows to collide. On the live
 * database there were fourteen, all of which became null `exec_id` the moment V4 applied — so if
 * the claim were wrong the second row would have failed the deploy, on the one database in the
 * estate whose contents cannot be recovered.
 */
@Testcontainers(disabledWithoutDocker = true)
class SchemaUpgradeTest {
    companion object {
        /** The version deployed before the one under test — the state a real upgrade starts from. */
        private val PREVIOUS = MigrationVersion.fromVersion("3")

        @Container
        @JvmField
        val oracle: org.testcontainers.oracle.OracleContainer =
            org.testcontainers.oracle
                .OracleContainer("gvenzl/oracle-free:23-slim-faststart")
                .withStartupTimeout(java.time.Duration.ofMinutes(3))
    }

    private fun connect(): Connection = DriverManager.getConnection(oracle.jdbcUrl, oracle.username, oracle.password)

    private fun flyway(target: MigrationVersion?) =
        Flyway
            .configure()
            .dataSource(oracle.jdbcUrl, oracle.username, oracle.password)
            .cleanDisabled(false)
            .apply { target?.let { target(it) } }
            .load()

    /**
     * The schema as it stood before the migration under test, wiped clean first.
     *
     * The two assertions are what stop this class being vacuous. If `target` were ignored the setup
     * would silently land on the current schema, every test below would exercise latest → latest,
     * and they would all pass while proving nothing about an upgrade. So the starting state is
     * asserted rather than assumed: the version is the previous one, and the column the migration
     * under test adds does not exist yet.
     */
    private fun startFromPreviousVersion() {
        flyway(PREVIOUS).apply { clean() }.migrate()

        assertEquals(PREVIOUS, flyway(null).info().current().version, "setup did not stop at the previous version")
        assertEquals(
            0,
            query("SELECT COUNT(*) FROM user_tab_columns WHERE table_name = 'FILLS' AND column_name = 'EXEC_ID'")
                .single()
                .single()
                .toString()
                .toInt(),
            "exec_id already exists, so this is not the pre-upgrade schema",
        )
    }

    /** A fill as the pre-V4 schema held one: every column except the one V4 adds. */
    private fun insertPreUpgradeFill(offset: Long) {
        connect().use { connection ->
            connection
                .prepareStatement(
                    "INSERT INTO fills (source_topic, source_partition, source_offset, symbol, price, " +
                        "signed_size, maker_order_id, taker_order_id, time_millis) " +
                        "VALUES ('orderbook.fills', 0, ?, 'SIM', 101.5, 5, 1, 2, 1720620000000)",
                ).use { statement ->
                    statement.setLong(1, offset)
                    statement.executeUpdate()
                }
        }
    }

    private fun query(sql: String): List<List<Any?>> =
        connect().use { connection ->
            connection.prepareStatement(sql).use { statement ->
                statement.executeQuery().use { rows ->
                    val columns = rows.metaData.columnCount
                    buildList {
                        while (rows.next()) add((1..columns).map { rows.getObject(it) })
                    }
                }
            }
        }

    @Test
    fun `the current schema applies to the previous one with rows already in it`() {
        startFromPreviousVersion()
        insertPreUpgradeFill(offset = 1)

        flyway(null).migrate()

        assertEquals(
            1,
            query("SELECT COUNT(*) FROM fills")
                .single()
                .single()
                .toString()
                .toInt(),
        )
    }

    @Test
    fun `existing rows survive the upgrade with the new column null, not defaulted`() {
        // Null is the correct value: these records predate the producer stamping an execution id,
        // and inventing one would make them look deduplicable by an identity they never carried.
        startFromPreviousVersion()
        insertPreUpgradeFill(offset = 1)

        flyway(null).migrate()

        assertNull(query("SELECT exec_id FROM fills").single().single())
    }

    @Test
    fun `the new unique index tolerates every pre-existing row being null`() {
        // The claim V4's comment makes, and the one an empty-database migration can never test.
        // Fourteen rows on the live database became null exec_id when this applied; had Oracle
        // treated nulls as colliding, the second would have failed the deploy.
        startFromPreviousVersion()
        (1L..14L).forEach(::insertPreUpgradeFill)

        flyway(null).migrate()

        assertEquals(
            14,
            query("SELECT COUNT(*) FROM fills WHERE exec_id IS NULL")
                .single()
                .single()
                .toString()
                .toInt(),
        )
    }

    @Test
    fun `the upgrade leaves the schema at the same version a clean install reaches`() {
        // A migration that applies but lands somewhere else is how two environments drift while
        // both report success.
        startFromPreviousVersion()
        insertPreUpgradeFill(offset = 1)
        flyway(null).migrate()
        val upgraded = flyway(null).info().current().version

        flyway(null).apply { clean() }.migrate()

        assertEquals(flyway(null).info().current().version, upgraded)
    }
}
