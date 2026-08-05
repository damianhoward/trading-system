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
