# trading-system

[![CI](https://github.com/damianhoward/trading-system/actions/workflows/ci.yml/badge.svg)](https://github.com/damianhoward/trading-system/actions/workflows/ci.yml)
[![CodeQL](https://github.com/damianhoward/trading-system/actions/workflows/codeql.yml/badge.svg)](https://github.com/damianhoward/trading-system/actions/workflows/codeql.yml)
[![codecov](https://codecov.io/gh/damianhoward/trading-system/graph/badge.svg)](https://codecov.io/gh/damianhoward/trading-system)

Post-trade integration over the [orderbook](https://github.com/damianhoward/orderbook) fill stream:
consumes fills off Kafka into a durable fill ledger in an Oracle Autonomous Database, derives net
positions in the same transaction, reprices the book through the
[risk-engine](https://github.com/damianhoward/risk-engine) library, and serves a live
positions/risk/PnL dashboard.

**▶ Live: https://trading.damianhoward.com** — submit an order that crosses on
[the live order book](https://orderbook.damianhoward.com) and watch the position, valuation,
VaR, and day PnL update here as the fill arrives.

```
orderbook.fills (Kafka) ──┬─► FillConsumer (seek from ledger) ──► TradeCapture ──► fill ledger + positions (Oracle, one txn)
                          │        │                                           │
                          │        ▼ on poison (parse failure) only            ▼ every applied fill
                          │  orderbook.fills.DLT (confirmed)          RiskGateway (risk-engine)
                          │                                                    │
                          │                                     DashboardServer ──► SSE ──► browser
                          │                                                    ▲
                          └─► FillConsumer (seek from ledger) ──► LimitsChecker
```

Matching and pricing are versioned library dependencies, not code in this repo:

```groovy
implementation 'com.github.damianhoward:orderbook:v3.0.0'
implementation 'com.github.damianhoward:risk-engine:v2.0.0'
```

## The book of record: idempotent fill application

Delivery off Kafka is at-least-once, so the same fill can arrive more than once — a consumer
retry, a redelivery after a crash, a replay after a restart. Applying it more than once would
corrupt the positions, so application is idempotent at the database:

- Every fill is inserted into a `fills` ledger keyed by its stream coordinates
  `(source_topic, source_partition, source_offset)`, and the `positions` row moves by the fill's
  signed size **in the same transaction**. A crash can never persist one without the other.
- A replayed record hits the ledger's primary key (ORA-00001), is reported as a duplicate, and
  changes nothing. The dashboard counts these under "Replays dropped".
- The producer's `execId` is stored under its own unique index: coordinates identify a
  _record_, `execId` identifies the _execution_, so the same economic fill republished at new
  coordinates — a dead-letter replay, a redelivery through another topic — is still a
  duplicate. Records published before the id existed dedupe by coordinates alone.
- In-memory state updates only from the committed row, after the transaction — a retried
  handler cannot move a position twice, because the second attempt is a duplicate by then.

`JdbcPositionStoreTest` proves the sequences against a real Oracle (Testcontainers): replayed
coordinates are dropped, and a simulated failure between the ledger insert and the position
merge rolls the whole transaction back, leaving the coordinates free for the retry.

## Fill consumption and failure handling

`FillConsumer` runs a plain `kafka-clients` poll loop on its own thread. There is no consumer
group and no offset commit: the fill ledger is the only checkpoint. Each partition is assigned
directly and starts just past the ledger's high-water mark — or from the beginning where nothing
is recorded, so a fresh or rebuilt database re-derives its state from the retained stream. A
group commit would only add a second, competing account of how far the book of record has read;
the migration that introduced the ledger (`V3`) retired the group by emptying `positions` and
letting the stream rebuild it through the ledger.

Each record passes through `RetryingHandler`, which splits poison from transient — the two
failures deserve opposite treatment:

- A record that fails to **parse** goes straight to `orderbook.fills.DLT` — malformed input
  never heals, so retrying it only stalls the stream.
- A valid record whose **handling** fails (the database blinked) is retried 3 times, 500 ms
  apart, and on exhaustion the process halts rather than skip it: a valid economic event must
  never be lost to a transient outage, so the consumer exits, systemd restarts it, and the
  restart seeks past the ledger's high-water mark — straight back to the stuck fill, into an
  application the ledger keeps idempotent. A database outage therefore shows as a restarting,
  not-ready service — never as a silently incomplete book. The DLT is for records that can
  never apply, not records that could not apply _yet_.
- Dead-letter publication is **confirmed**: `publish` blocks until the broker acknowledges the
  send. An unacknowledged send throws instead of being counted and forgotten — the process exits
  rather than commit past a record that is in neither stream. A record is either applied, on the
  DLT, or ahead of the committed offset; never in none of those places.
- Dead-lettered records carry the untouched original payload plus `dlt.error.*` and
  `dlt.source.*` headers (exception, source topic/partition/offset) for inspection and replay.
  The dashboard's status bar flags a non-zero dead-letter count.

### Dead-letter replay

`trading-system replay-dlt` (the same binary, run with the service's environment) replays
dead-lettered records back onto the fills topic: records whose payload now parses — a fill
dead-lettered by a since-fixed defect — are republished with their key, payload, and provenance
headers untouched, while still-malformed records stay on the DLT. Every send is confirmed and
logged with its DLT coordinates. A replayed copy lands at new stream coordinates, but its
payload keeps its `execId`, so the ledger recognises a second replay of the same execution as a
duplicate — replay is idempotent for any record carrying the id. Only pre-`execId` records need
the older discipline: replay once, then verify the dashboard's position and dead-letter counts.

The fill schema is orderbook's versioned egress JSON (`v`, `execId`, `symbol`, `price`, `size`,
`makerOrderId`, `takerOrderId`, `aggressor`, `ts`), parsed strictly — an unknown schema version
or a wrongly-typed field is poison, not a guess. `execId` is optional: records published before
the producer stamped it parse with a null identity.

## Positions

`PositionBook` holds the net signed quantity per symbol — a BID aggressor bought, an OFFER
aggressor sold — as an in-memory mirror of the store's committed truth: rows enter it only from
committed transactions, so it can never run ahead of the database. At startup it warms from the
`positions` table, which the ledger keeps consistent by construction. Schema is managed by
Flyway.

## Limits

A second `FillConsumer` feeds `LimitsChecker`, an independent exposure view over the same fill
stream — it never reads the position book — checking two ceilings on every fill: absolute net
position and notional (|net quantity| × last price). Crossing a ceiling in either direction
records a breach or clear event, stamped with the fill's own timestamp; the dashboard shows
current utilisation per symbol and the recent event history.

Limits state is restart-safe through the fill ledger, exactly like the positions path: at
startup the checker replays the persisted fills (rebuilding exposures **and** the breach
history — events carry each fill's own time), and the consumer attaches to the live stream past
the ledger's high-water mark. Both views resume from the same durable truth after a restart,
the dashboard's `sync` block says whether they have read to the same stream position since, and
`/readyz` refuses to call the service ready when they stay apart.

Malformed records on this path are counted and skipped rather than dead-lettered — the positions
consumer owns the DLT, and a second publisher would duplicate every poison record.

## Risk and PnL

On every applied fill, `RiskGateway` prices **every position**, each in its own market: a
risk-engine `Portfolio` per symbol, marked at that symbol's last traded price, through
`RiskReportAssembler` — mark-to-market valuation, bump-and-reprice Greeks, parametric and
historical-simulation VaR/ES, and the day's PnL attribution with its explicit residual. Every
number comes from risk-engine's validated calculators; this repo adds no pricing maths.

The book strip sums only what is honest to sum across single-underlier reports: valuation,
gross notional, and day PnL are currency amounts and add; Greeks and VaR stay per symbol,
because summing share-count deltas across underliers, or taking a quantile across
independently-scenarioed symbols, would label numbers the market model does not price.

Day PnL measures from `SessionOpens`: each symbol's open is the price of its first fill on its
latest trading day (UTC, by the fill's own timestamp), derived purely from the ledger fills that
built the book — so a restart warms the same opens back and the PnL clock survives the process.
A symbol with no fill on its latest day carries no PnL claim rather than a zero.

## Dashboard and operational truth

A dependency-free JDK `HttpServer`: `GET /api/state` returns the current snapshot as JSON and
`GET /api/stream` is an SSE feed pushing a fresh snapshot on every fill. The front end is a thin
renderer of the snapshot JSON, in the same terminal-style UI as the orderbook and risk-engine
live sites.

Two endpoints report health at different depths:

- `/healthz` — liveness: the web process answers.
- `/readyz` — readiness: every consumer thread alive, assigned, and recently polling; the
  database answering; the positions and limits views at the same stream offset (independent
  consumers may sit apart mid-burst, so divergence gets a 30 s grace window — offsets still
  apart after that mean a projection is stuck); every derived view reconciling against the ledger
  it comes from; dead-letter publish/failure counters. Returns 503 with the failing component
  named when the pipeline is broken or the projections disagree, whatever the web process says.
  Deploys gate on this, so a deploy whose consumers cannot attach — or whose views cannot
  converge — fails instead of going green.

### Reconciliation

Equal offsets prove two views have read the same amount of stream. They do not prove either holds
the right number: a projection that has drifted from its fills sits at the correct offset with the
wrong quantity, and nothing in the request path would notice.

So a background pass asserts the conservation property — **every derived view equals the signed
sum of the ledger fills behind it** — every minute, and `/readyz` fails when it does not hold,
naming the view, each symbol, both quantities and the gap between them.

There are three such views, and they fail differently:

| View            | What it is                                     | How a drift shows                                                                       |
| --------------- | ---------------------------------------------- | --------------------------------------------------------------------------------------- |
| `positions`     | the table, moved in the fill's own transaction | a bad restore, a manual correction, a migration, a bad merge                            |
| `position_book` | the in-memory mirror                           | wrong valuation, Greeks and VaR — the risk report is priced from it, not from the table |
| `limits`        | the limits consumer's independent exposure map | breach events against exposures nothing else in the service reads                       |

The write path maintains `positions` by construction, inserting the fill and moving the position
in one transaction, which is precisely why it is worth checking. `limits` is the opposite case:
it never reads the position book, and that independence — the point of running it in its own
consumer group — is also why nothing else would notice it disagreeing.

The table is read alongside the ledger sum in one SQL statement, so they describe the same
instant and a fill committing mid-check cannot manufacture a divergence that was never real. An
in-memory view cannot share that instant, so it gets the equivalent guarantee a different way:
the same statement returns the ledger's high-water offset, and a view is judged only when it sits
there. A view at a different offset is reported **inconclusive** rather than divergent — comparing
one mid-flight would flag a divergence for every fill in transit, and a check that cries wolf gets
turned off.

Inconclusive is not a free pass. A view that can never be judged asserts nothing, so the probe
fails once one has been continuously unjudgeable for three passes. The budget is counted in passes
rather than seconds because a pass is what samples the view.

The probe also fails when the check itself goes stale — a checker that has stopped leaves a
passing result behind it, which reads exactly like a book that reconciles.

The snapshot itself carries a `sync` block — each consumer path's last stream offset and fill
timestamp, whether the two views are coherent, how many replays the ledger dropped, and how many
records were dead-lettered this session. The status bar renders it, along with the age of the
current mark, so a quiet stream shows as an ageing mark rather than passing for fresh.

A consumer that dies on an unexpected exception is never a silent zombie: readiness reports the
fatal error, and the process exits so systemd restarts it into a safe replay.

## Configuration

| Variable                             | Default                          | Purpose                            |
| ------------------------------------ | -------------------------------- | ---------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS`            | `127.0.0.1:9092`                 | Broker to consume from             |
| `FILLS_TOPIC` / `FILLS_DLT_TOPIC`    | `orderbook.fills` / `…fills.DLT` | Source and dead-letter topics      |
| `LIMIT_MAX_POSITION`                 | `50`                             | Absolute net position ceiling      |
| `LIMIT_MAX_NOTIONAL`                 | `5000`                           | Notional exposure ceiling          |
| `DB_URL` / `DB_USER` / `DB_PASSWORD` | required                         | Oracle connection (wallet TNS URL) |
| `PORT`                               | `8082`                           | Dashboard HTTP port                |

Neither consumer takes a group id — both derive their start position from the fill ledger.

## Build

```bash
./gradlew clean build
```

JDK 25 via Gradle toolchain. The suite includes real-broker tests (fills in, positions out,
poison to the DLT; the seek-mode limits consumer fanning out over one topic) and real-database
store tests (Oracle Free) covering replay, retry, and mid-transaction failure — all via
Testcontainers; a local Docker daemon is required for the full `check`. Coverage is gated at 90%
instruction with only the process entry point excluded.
