# Millisecond Mind

**A streaming reflex plus an AI brain that detects, verifies, and fixes a payment-gateway outage in seconds with no human paged.**

A payment gateway starts failing mid-checkout. An Apache Beam pipeline (the *reflex*) notices within seconds. It wakes a Gemini agent (the *brain*), which compares the live numbers against 30 days of BigQuery history, confirms the outage is real, reroutes traffic to a healthy gateway, and posts an incident report with its evidence.

> Think of touching a hot stove: your spinal cord pulls your hand back before you think (reflex), then your brain works out what happened (reasoning). Fast and simple watches everything; smart and slow is woken only when it matters.

---

## Architecture

```
 publisher.py ──► Pub/Sub ──────────► pipeline.py (Beam) ──► Redis (reflex store)
 (synthetic        payment-events     60 s sliding windows     ▲          │
  checkouts)                          every 5 s                │          │ routing override
      ▲                                   │ breach?            │          ▼
      │                                   ▼                    │     agent.py (Gemini) ──► BigQuery
      │                               Pub/Sub ─────────────────┴──►  investigates          30-day history
      │                               agent-triggers                  │
      │                                                               ▼
      └──────── reads routing:* keys from Redis ◄────────── reroute + incident_reports/*.md
```

| Component | File | Role |
|---|---|---|
| History | `generate_history.py` | Creates the BigQuery table and loads 30 days of hourly per-gateway stats (the agent's idea of "normal"). |
| Traffic | `publisher.py` | Publishes ~200 synthetic checkout events/s to Pub/Sub and can inject a FastPay outage. Honors the agent's reroute. |
| Reflex | `pipeline.py` | Beam streaming job: windows, compacts, writes live state to Redis, and raises alerts on breach. |
| Brain | `agent.py` | Gemini agent with tools for Redis, BigQuery, rerouting, and reporting. |
| Settings | `config.py` | All names, thresholds, and limits. Every value can be overridden with an env var. |

### Google Cloud services used
- **Pub/Sub**: `payment-events` (raw events) and `agent-triggers` (alerts)
- **BigQuery**: `checkout_ops.gateway_hourly_stats` (history, partitioned by day)
- **Vertex AI / Gemini**: agent reasoning and tool calling
- **Apache Beam**: DirectRunner locally, so there is no Dataflow cost (runs on Dataflow unchanged)
- **Redis**: the low-latency "reflex store" shared by the pipeline, agent, and publisher

---

## How the demo plays out

1. **History exists.** BigQuery holds 30 days of hourly stats for three gateways: FastPay (60% of traffic), SecurePay (30%), and QuickPay (10%). Normal failure rates are under 1%. The data includes a daily traffic curve, a weekly Tuesday flash sale, and one past QuickPay incident for realism.
2. **Normal traffic.** `publisher.py` sends ~200 events/s. About 1% of events arrive 60–90 s late to simulate flaky mobile networks, and Beam handles them with event-time windows and allowed lateness.
3. **Outage injected.** After `--fail-after` seconds (default 90), 78% of FastPay payments fail, mostly with `504_TIMEOUT` and 3.5–6 s latency.
4. **Reflex fires.** The pipeline summarizes each gateway over 60 s sliding windows (every 5 s) and writes the newest summary to Redis at `gw_state:<gateway>`, with a 5 min TTL. When a gateway's error rate is **≥ 20%** over **≥ 200 events**, it publishes an alert to `agent-triggers`. A 3 min cooldown key suppresses duplicate alerts.
5. **Brain investigates.** The agent:
   1. Reads live state for the alerting gateway and the alternatives.
   2. Describes the BigQuery table and writes a baseline query for the same `hour_of_day` over the last 30 days.
   3. Dry-runs the query, then executes it.
   4. Decides. In a real run, FastPay was live at **24.3%** against a baseline of **0.79%** (≈ 30×) over 2,893 events, so it is a real outage.
   5. Calls `reroute_traffic(FastPay → SecurePay)`.
   6. Posts a Markdown incident report with the evidence, the exact SQL, and the action taken.
6. **Recovery.** The publisher reads `routing:disabled_gateway` / `routing:target_gateway` from Redis, shifts FastPay's share to SecurePay, and checkout failures drop back to green. Alert-to-fix took **~30 s** in the recorded runs.

Sample output lives in `incident_reports/` and `logs/` (`run1/`, `run2/`, and Redis snapshots before and after the reroute).

---

## Guardrails

The safety rules are enforced in code, not only in the prompt.

| Guardrail | Where | What it does |
|---|---|---|
| Read-only SQL | `agent.py` `_is_read_only` | Only `SELECT`/`WITH` is allowed. DML and DDL keywords are rejected. |
| Dry run first | system prompt + tool | The agent validates SQL and sees bytes scanned before executing. |
| Byte cap | `MAX_BYTES_BILLED` | Each query is hard-capped at 100 MB billed. |
| Partition filter required | BigQuery table setting | Full-table scans are impossible (`require_partition_filter = True`). |
| Retry budget | `MAX_QUERY_ERRORS = 3` | After 3 failed queries the agent stops and escalates to a human. |
| Healthy target only | `reroute_traffic` | Refuses unless the target's live error rate is < 5%. |
| Anti-flapping | `routing:cooldown` | Allows at most one routing change per 10 minutes. |
| Auto-expiry | `routing:*` keys | A reroute expires after 30 minutes, so it is reversible by default. |
| Bounded tool loop | `maximum_remote_calls=15` | The agent cannot loop forever. |
| No invented numbers | system prompt | Every number in the report must come from a tool result. |
| Dead-letter queue | `pipeline.py` `ParseEvent` | Malformed events are logged, not crashed on. |
| Idempotent state | `WriteReflexState` | Only newer windows overwrite Redis, so replays are harmless. |

### Grounding experiment
`python agent.py --once FastPay --no-grounding` hides the column and table descriptions and uses a vaguer prompt. The agent then makes more SQL mistakes, which shows why schema metadata such as the documented `failure_rate` definition and partition column matters for agents.

---

## Prerequisites

- Python **3.10+**
- A Google Cloud project with billing enabled
- `gcloud` CLI, authenticated: `gcloud auth login` and `gcloud auth application-default login`
- Docker (for Redis)

## Setup

```bash
# 1. Python environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Environment variables (keep these in a local *.env file; it's git-ignored)
export PROJECT_ID=<your-gcp-project-id>
export GOOGLE_CLOUD_PROJECT=$PROJECT_ID
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_LOCATION=us-central1
export GEMINI_MODEL=gemini-2.5-flash      # use a current Flash model
export PYTHONUNBUFFERED=1
unset GEMINI_API_KEY GOOGLE_API_KEY        # force Vertex AI instead of an AI Studio key

# 3. Enable APIs and create Pub/Sub topics + subscriptions
./setup.sh

# 4. Start Redis
docker run -d --name reflex-redis -p 6379:6379 redis:7

# 5. Load 30 days of history into BigQuery (load jobs are free)
python generate_history.py
```

## Running the demo

Use three terminals, each with the venv activated and the env vars set:

```bash
# Terminal 1: the reflex
python pipeline.py --runner=DirectRunner --streaming

# Terminal 2: the brain
python agent.py --listen

# Terminal 3: traffic + outage
python publisher.py --rate 200 --fail-after 90
```

Watch terminal 3 turn red at ~t+90s, terminal 1 log `<-- BREACH` and `ALERT`, terminal 2 investigate and reroute, and terminal 3 turn green again with `routing: FastPay -> SecurePay`.

### Useful flags and commands

| Command | Purpose |
|---|---|
| `python publisher.py --fail-after -1` | Run without an outage |
| `python publisher.py --duration 300` | Run for a shorter time (default 900 s) |
| `python agent.py --once FastPay` | Trigger the agent manually, for retakes |
| `python agent.py --once FastPay --no-grounding` | Show the ungrounded agent struggling |
| `docker exec reflex-redis redis-cli DEL routing:cooldown routing:disabled_gateway routing:target_gateway` | Reset routing between runs |
| `docker exec reflex-redis redis-cli --scan` | Inspect the reflex store |

---

## Configuration

All values are in `config.py` and can be overridden with environment variables:

| Setting | Default | Meaning |
|---|---|---|
| `PROJECT_ID` | — | GCP project |
| `BQ_DATASET` / `BQ_TABLE` | `checkout_ops` / `gateway_hourly_stats` | History table |
| `BQ_LOCATION` | `asia-south1` | BigQuery location |
| `EVENTS_TOPIC` / `EVENTS_SUB` | `payment-events` / `payment-events-beam` | Raw events |
| `ALERTS_TOPIC` / `ALERTS_SUB` | `agent-triggers` / `agent-triggers-sub` | Alerts to the agent |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Reflex store |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Agent model |
| `ERROR_RATE_THRESHOLD` | `0.20` | Alert when window error rate ≥ 20% |
| `MIN_SAMPLE_SIZE` | `200` | …and the window has at least this many events |
| `ALERT_COOLDOWN_S` | `180` | No repeat alert for the same gateway within 3 min |
| `MAX_BYTES_BILLED` | 100 MB | Hard cap per agent query |

### Redis keys

| Key | Written by | TTL | Purpose |
|---|---|---|---|
| `gw_state:<gateway>` | pipeline | 5 min | Latest window summary (error rate, sample size, p95, top error) |
| `alert_cooldown:<gateway>` | pipeline | 3 min | Deduplicates alerts |
| `routing:disabled_gateway` / `routing:target_gateway` | agent | 30 min | The reroute; read by the publisher |
| `routing:cooldown` | agent | 10 min | Anti-flapping |

---

## Troubleshooting

- **Windows never reach 200 events locally.** The DirectRunner pulls only 10 Pub/Sub messages per bundle. `pipeline.py` patches this to 1,000 via `raise_direct_runner_pull_size()`. This only affects local runs.
- **Gemini 429 `RESOURCE_EXHAUSTED`.** Each tool turn is a separate model request (6–10 per investigation). Wait about 60 s, reset the routing keys, and rerun `python agent.py --once FastPay`. Using Vertex AI (the env vars above) avoids AI Studio free-tier limits.
- **`reroute_traffic` refused.** Either no alternative gateway has a live error rate below 5%, or a reroute happened within the last 10 minutes. Reset the routing keys to retry.
- **`no live state for <gateway>`.** The pipeline isn't running, or its state expired (5 min TTL).
- **BigQuery "partition filter required" error.** This is expected when a query omits `hour_ts`. The agent is told to filter on it and retries within its budget.

## Cost

- BigQuery: a ~2,200-row table. Load jobs are free, and queries are capped at 100 MB.
- Beam runs locally on the DirectRunner, so there is no Dataflow charge.
- Pub/Sub and Gemini usage for a single 15-minute run is minimal.

## Teardown

```bash
./teardown.sh   # deletes Pub/Sub topics/subscriptions, the BigQuery dataset, and the Redis container
```

## Repository layout

```
agent.py               Gemini agent + tools (the brain)
pipeline.py            Beam streaming pipeline (the reflex)
publisher.py           Synthetic traffic + outage injection
generate_history.py    BigQuery history table + 30 days of data
config.py              Shared settings
setup.sh / teardown.sh GCP resource lifecycle
incident_reports/      Reports posted by the agent
logs/                  Recorded runs and Redis snapshots
screenshots/           Demo screenshots (01–11)
millisecond_mind.pptx  Presentation deck
```
