#!/usr/bin/env bash
# Remote work specific to this service, run on the box by the shared deploy workflow after the unit
# sync and before the release switch. DEPLOY_DIR holds this repository's deploy/ directory as
# shipped; the workflow removes it afterwards, so nothing here cleans up after itself.
#
# The broker's unit lives in this repository because it is this repository's dependency and nothing
# else on the estate uses it. It was previously box-only state: edited by hand, described nowhere,
# and carrying a defect for its whole life that no review could have caught because no reviewer
# could see it.
set -euo pipefail

# Box 2's whole Caddy configuration, which was in no repository at all: a rebuilt box would have
# come back without TLS, without the security headers, and without the access log the estate's
# analytics collection reads.
if ! cmp -s "$DEPLOY_DIR/Caddyfile" /etc/caddy/Caddyfile; then
  # Validate before installing: a bad Caddyfile that reaches /etc and gets reloaded takes the site
  # down, and nothing in the deploy would put it back.
  sudo caddy validate --config "$DEPLOY_DIR/Caddyfile" --adapter caddyfile
  sudo cp /etc/caddy/Caddyfile "/etc/caddy/Caddyfile.bak-$(date +%Y%m%d%H%M%S)"
  sudo cp "$DEPLOY_DIR/Caddyfile" /etc/caddy/Caddyfile
  sudo systemctl reload caddy
  echo "Caddyfile updated and caddy reloaded"
fi

# The broker's own configuration — listeners, the SASL protocol map, retention, log dirs. It holds
# no credential (the SCRAM users live in Kafka's own metadata), so it belongs here. Without it a
# rebuilt broker comes back on defaults: no VCN listener, no SASL protocol map, and orderbook
# unable to reach it from box 1.
if ! cmp -s "$DEPLOY_DIR/kraft-single.properties" /home/ubuntu/kafka/config/kraft-single.properties; then
  cp /home/ubuntu/kafka/config/kraft-single.properties "/home/ubuntu/kafka/config/kraft-single.properties.bak-$(date +%Y%m%d%H%M%S)"
  cp "$DEPLOY_DIR/kraft-single.properties" /home/ubuntu/kafka/config/kraft-single.properties
  echo "kraft-single.properties updated — restart the broker deliberately to apply it"
fi

if ! cmp -s "$DEPLOY_DIR/kafka.service" /etc/systemd/system/kafka.service; then
  sudo cp "$DEPLOY_DIR/kafka.service" /etc/systemd/system/kafka.service
  sudo systemctl daemon-reload
  # Reloaded but deliberately not restarted. A broker restart is an outage for every consumer on
  # the estate, and an application release is the wrong moment to take one: the running broker
  # keeps its current settings until someone restarts it on purpose. Heap changes in particular
  # only take effect at JVM start, so a unit edit here is a staged change, not an applied one.
  echo "kafka.service updated and systemd reloaded — restart the broker deliberately to apply it:"
  echo "  sudo systemctl restart kafka"
fi
