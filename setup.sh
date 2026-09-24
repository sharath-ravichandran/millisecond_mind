#!/usr/bin/env bash
# One-time GCP setup. Usage: PROJECT_ID=my-project ./setup.sh
set -euo pipefail
: "${PROJECT_ID:?set PROJECT_ID first}"
gcloud config set project "$PROJECT_ID"
gcloud services enable pubsub.googleapis.com bigquery.googleapis.com aiplatform.googleapis.com
gcloud pubsub topics create payment-events || true
gcloud pubsub subscriptions create payment-events-beam --topic=payment-events --ack-deadline=60 || true
gcloud pubsub topics create agent-triggers || true
gcloud pubsub subscriptions create agent-triggers-sub --topic=agent-triggers --ack-deadline=300 || true
echo "Done. Next: docker run -d --name reflex-redis -p 6379:6379 redis:7"
