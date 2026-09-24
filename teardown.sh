#!/usr/bin/env bash
# Delete everything the demo created. Usage: PROJECT_ID=my-project ./teardown.sh
set -uo pipefail
: "${PROJECT_ID:?set PROJECT_ID first}"
gcloud pubsub subscriptions delete payment-events-beam agent-triggers-sub --project "$PROJECT_ID"
gcloud pubsub topics delete payment-events agent-triggers --project "$PROJECT_ID"
bq rm -r -f -d "$PROJECT_ID:checkout_ops"
docker rm -f reflex-redis
