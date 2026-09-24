"""Shared settings. Override any of these with environment variables."""
import os

PROJECT_ID = os.environ.get("PROJECT_ID", "your-gcp-project-id")
DATASET = os.environ.get("BQ_DATASET", "checkout_ops")
TABLE = os.environ.get("BQ_TABLE", "gateway_hourly_stats")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "asia-south1")

EVENTS_TOPIC = os.environ.get("EVENTS_TOPIC", "payment-events")
EVENTS_SUB = os.environ.get("EVENTS_SUB", "payment-events-beam")
ALERTS_TOPIC = os.environ.get("ALERTS_TOPIC", "agent-triggers")
ALERTS_SUB = os.environ.get("ALERTS_SUB", "agent-triggers-sub")

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

# Check the current model list before running and set GEMINI_MODEL to a current Flash model.
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

GATEWAYS = ["FastPay", "SecurePay", "QuickPay"]
ERROR_RATE_THRESHOLD = 0.20   # alert when a gateway's window error rate >= 20%
MIN_SAMPLE_SIZE = 200         # ...and the window has at least this many events
ALERT_COOLDOWN_S = 180        # don't re-alert the same gateway within 3 minutes
MAX_BYTES_BILLED = 100 * 1024 * 1024  # 100 MB hard cap per agent query
