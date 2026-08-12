package com.damianhoward.tradingsystem.health

import com.damianhoward.tradingsystem.consume.ConsumerHealth
import com.damianhoward.tradingsystem.consume.ConsumerProgress
import com.damianhoward.tradingsystem.position.Reconciliation
import com.damianhoward.tradingsystem.position.ReconciliationChecker
import java.time.Clock
import java.time.Duration

/**
 * The service's operational truth, aggregated for `/readyz`: every consumer thread alive,
 * assigned, and polling recently; the database answering; the two views describing the same
 * stream position; the dead-letter counters in the open. `/healthz` proves the web process
 * answers — this proves the system is doing its job, so a server whose projections disagree is
 * not safe to serve, whatever the process state says.
 *
 * View coherence gets a grace window: the positions and limits consumers are independent, so
 * mid-burst they legitimately sit a few records apart for well under a second. Offsets that stay
 * unequal past [coherenceGrace] mean a view is stuck, and the probe goes 503 naming both
 * positions. Past dead-letter failures are reported but do not fail the probe (an unacknowledged
 * send already fails fast at the moment it happens).
 *
 * Reconciliation asks the question coherence cannot. Equal offsets prove the two views have read
 * the same amount of stream, not that either holds the right number; a position that drifted from
 * its ledger sits at the correct offset with the wrong quantity. So the probe also fails when the
 * book stops agreeing with the fills it derives from, or when the check establishing that has gone
 * stale — no grace window, because unlike the two consumers there is no race that makes a
 * divergence briefly legitimate. It is read at one instant from one statement, so it is either
 * true or it is a defect.
 */
class Readiness(
    private val consumers: List<ConsumerHealth>,
    private val databaseOk: () -> Boolean,
    private val deadLettersPublished: () -> Long,
    private val deadLettersFailed: () -> Long,
    private val positionsView: () -> ConsumerProgress?,
    private val limitsView: () -> ConsumerProgress?,
    private val reconciliation: () -> Reconciliation?,
    private val maxPollAge: Duration = MAX_POLL_AGE,
    private val coherenceGrace: Duration = COHERENCE_GRACE,
    private val maxReconciliationAge: Duration = ReconciliationChecker.MAX_AGE,
    private val clock: Clock = Clock.systemUTC(),
) {
    data class Probe(
        val ready: Boolean,
        val json: String,
    )

    @Volatile
    private var incoherentSinceMillis: Long? = null

    fun probe(): Probe {
        val consumerStates = consumers.map { consumerState(it) }
        val database = databaseOk()
        val views = viewsState()
        val ledger = ledgerState()
        val ready = database && views.second && ledger.second && consumerStates.all { it.second }
        val json =
            """{"ready":$ready,"consumers":{${consumerStates.joinToString(",") { it.first }}},""" +
                """"database":{"ok":$database},"views":${views.first},"ledger":${ledger.first},""" +
                """"deadLetters":{"published":${deadLettersPublished()},"failed":${deadLettersFailed()}}}"""
        return Probe(ready, json)
    }

    /**
     * The conservation check's standing verdict. Divergences are named individually with both
     * quantities: an operator seeing 503 needs the symbol and the size of the gap, and there is
     * nothing secret in either — they are the same numbers the public dashboard already serves.
     */
    private fun ledgerState(): Pair<String, Boolean> {
        val latest = reconciliation()
        val ageMillis = latest?.let { clock.millis() - it.checkedAtMillis }
        val fresh = ageMillis != null && ageMillis <= maxReconciliationAge.toMillis()
        val ok = fresh && latest!!.agrees
        val divergences =
            latest?.divergences.orEmpty().joinToString(",") {
                """{"symbol":${quote(it.symbol)},"ledgerQuantity":${it.ledgerQuantity},""" +
                    """"positionQuantity":${it.positionQuantity},"difference":${it.difference}}"""
            }
        val json =
            """{"ok":$ok,"checkedAgeMillis":${ageMillis ?: "null"},""" +
                """"symbolsChecked":${latest?.symbolsChecked ?: "null"},"divergences":[$divergences]}"""
        return json to ok
    }

    private fun viewsState(): Pair<String, Boolean> {
        val positions = positionsView()?.offset
        val limits = limitsView()?.offset
        val coherent = positions == limits
        val incoherentFor =
            if (coherent) {
                incoherentSinceMillis = null
                null
            } else {
                val since = incoherentSinceMillis ?: clock.millis().also { incoherentSinceMillis = it }
                clock.millis() - since
            }
        val ok = coherent || incoherentFor!! <= coherenceGrace.toMillis()
        val json =
            """{"ok":$ok,"positionsOffset":${positions ?: "null"},"limitsOffset":${limits ?: "null"},""" +
                """"coherent":$coherent,"incoherentForMillis":${incoherentFor ?: "null"}}"""
        return json to ok
    }

    private fun consumerState(health: ConsumerHealth): Pair<String, Boolean> {
        val pollAge = health.pollAgeMillis()
        val polling = pollAge != null && pollAge <= maxPollAge.toMillis()
        val ok = health.threadAlive && health.assigned && polling && health.fatal == null
        val json =
            """"${health.name}":{"ok":$ok,"threadAlive":${health.threadAlive},"assigned":${health.assigned},""" +
                """"pollAgeMillis":${pollAge ?: "null"},"fatal":${health.fatal?.let { quote(it) } ?: "null"}}"""
        return json to ok
    }

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

    companion object {
        private val MAX_POLL_AGE: Duration = Duration.ofSeconds(30)
        private val COHERENCE_GRACE: Duration = Duration.ofSeconds(30)
    }
}
