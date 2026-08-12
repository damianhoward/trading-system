# Deploy

The capstone runs as a systemd JVM service behind Caddy at
**https://trading.damianhoward.com**, on its own box rather than beside the other services: it
sits next to the Kafka broker it consumes from, and it is the only service that holds the book
of record.

`.github/workflows/deploy.yml` is a thin caller of the estate's shared pipeline. That pipeline
runs `clean build`, packages the `installDist` distribution once, and ships those exact bytes —
no second build between test and release. The artifact unpacks into
`/srv/trading-system/releases/<commit>`, `/srv/trading-system/current` is moved onto it with a
symlink rename so a restart can never see a half-copied install, and success is gated on
`/readyz`. Three releases are retained.

The release procedure itself is not sent from here. It is a root-owned script on the box, kept with
the box's other privileged configuration, and CI asks for a release in five words with the bundle
on stdin.

## Readiness is deliberately slow here

The caller passes `hold-seconds: 30`, longer than the shared default and longer than the 30
second coherence grace in `Readiness`. This is the one setting in the file worth understanding.

`/readyz` answers 503 while the positions and limits views disagree. Those views are populated by
two independent consumer groups, so immediately after a restart they are briefly, legitimately
out of step — and a deploy that probed during that window would go green by asking the question
before the service could answer it honestly. Holding past the grace means readiness is measured
against a service that has had the chance to declare itself incoherent. The 2026-07-19 ORA-12838
incident is the argument for it: a deploy that reports success while the consumer is crash-looping
is worse than one that fails.

## Service

The unit is not in this repository. A systemd unit is a request to run anything as anyone, so a
deploy account able to install one holds root by another name — and this box holds the ledger, the
database wallet and the broker's SCRAM credential. `trading-system.service` therefore lives with
the box's other privileged configuration and is applied by an operator. What CI may do here is
restart the service, and nothing else.

That unit runs the launcher with `-Xmx128m` under `MemoryMax=448M`. The gap is intentional.
Peak observed resident is 183 MB — the highest on the estate, because the Oracle
driver and the Kafka consumer both hold buffers outside the heap — and the failure this service
must not have is being OOM-killed mid-transaction on a cap set too tight.

Sandboxing is tighter here than anywhere else on the estate, for a reason the unit records: this
process holds the Autonomous Database wallet and the Kafka SCRAM credential, and it owns the
ledger. It writes nothing to disk, so nothing is writable — the ledger and positions live in the
database, and the consumer's position in the stream lives in the ledger rather than in a consumer
group, which is why there is no local state to protect. `systemd-analyze security
trading-system.service` reports the result.

Configuration comes from `EnvironmentFile=/etc/trading-system/env` and is required at startup:
the port, the broker address, and the database triple. The service does not start with a partial
configuration, which is deliberate — a positions service that comes up pointing at nothing is
harder to notice than one that does not come up.

## Secrets

`DEPLOY_HOST`, `DEPLOY_USER` and `DEPLOY_SSH_KEY` are GitHub Actions secrets and are never in the
repository. The host key is pinned from [`known_hosts.pub`](known_hosts.pub) composed with the
secret address, rather than trusting whatever answers on it.

`DEPLOY_SSH_KEY` is a key of CI's own, not the operator's, and on the box it is pinned to a forced
command: it can ask for a release and can do nothing else — no shell, no file copy, no port
forward. The account behind it may run exactly one command as root, `systemctl restart
trading-system`. Both halves of that arrangement are host configuration, kept and applied outside
this repository.

## Rollback

Decided on the box, not by the runner. Release, health check and rollback are one remote script,
so a runner that dies between restart and check cannot leave a broken release serving with
nothing left to react to it. A release that does not answer `/readyz` has the symlink moved back
onto its predecessor and the service restarted. Retaining three releases is what makes the
predecessor still there to point at.
