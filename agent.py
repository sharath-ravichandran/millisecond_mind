"""Step 4: the brain. A Gemini agent with BigQuery tools and one bounded action.

Listen for alerts from the pipeline:   python agent.py --listen
Trigger once by hand (for retakes):   python agent.py --once FastPay
Provoke more SQL mistakes (for the 'opaque schema' screenshot):  add --no-grounding
"""
import argparse
import json
import os
import re
import time
from datetime import datetime, timezone

import redis
from google import genai
from google.cloud import bigquery, pubsub_v1
from google.genai import errors, types
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

import config

console = Console()
bq = bigquery.Client(project=config.PROJECT_ID)
r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
FULL_TABLE = f"{config.PROJECT_ID}.{config.DATASET}.{config.TABLE}"

STATE = {"t0": time.time(), "query_errors": 0, "grounded": True}
MAX_QUERY_ERRORS = 3


def _log(icon: str, msg: str, style: str = "white"):
    console.print(f"[dim]t+{time.time() - STATE['t0']:5.1f}s[/]  {icon} [{style}]{msg}[/]")


# ----------------------------- tools -----------------------------
def get_live_gateway_state(gateway: str) -> dict:
    """Read the latest 60-second window summary for a payment gateway from the live reflex store.

    Args:
        gateway: Gateway name: 'FastPay', 'SecurePay' or 'QuickPay'.
    """
    _log("⚡", f"get_live_gateway_state({gateway!r})", "cyan")
    raw = r.get(f"gw_state:{gateway}")
    if raw is None:
        return {"error": f"no live state for {gateway} (unknown gateway or pipeline not running)"}
    state = json.loads(raw)
    _log(" ", f"   error_rate={state['error_rate']:.1%}  n={state['sample_size']}  p95={state['p95_latency_ms']}ms", "cyan")
    return state


def describe_table(table_name: str) -> dict:
    """Return the schema of a BigQuery table: column names, types, descriptions and partitioning.

    Args:
        table_name: Fully qualified table name, project.dataset.table.
    """
    _log("🔎", f"describe_table({table_name!r})", "yellow")
    try:
        t = bq.get_table(table_name)
    except Exception as e:  # noqa: BLE001
        _log("✗", f"   {e}", "red")
        return {"error": str(e)}
    cols = [{"name": f.name, "type": f.field_type,
             **({"description": f.description} if STATE["grounded"] and f.description else {})}
            for f in t.schema]
    out = {"table": table_name, "columns": cols,
           "partition_column": t.time_partitioning.field if t.time_partitioning else None,
           "require_partition_filter": t.require_partition_filter}
    if STATE["grounded"]:
        out["table_description"] = t.description
    return out


def _is_read_only(sql: str) -> bool:
    s = re.sub(r"--.*?$", "", sql, flags=re.M).strip().lower()
    banned = ("insert", "update", "delete", "merge", "drop", "create", "alter", "truncate")
    return s.startswith(("select", "with")) and not any(re.search(rf"\b{w}\b", s) for w in banned)


def _query_error(e: Exception) -> dict:
    STATE["query_errors"] += 1
    msg = getattr(e, "message", None) or str(e)
    _log("✗", f"   BigQuery error: {msg[:160]}", "red")
    if STATE["query_errors"] >= MAX_QUERY_ERRORS:
        return {"error": msg, "note": "Retry budget exhausted. Stop querying and escalate to a human with what you know."}
    return {"error": msg, "note": "Read the error, fix the SQL and try again."}


def dry_run_query(sql: str) -> dict:
    """Validate a read-only BigQuery SQL query without running it. Returns bytes it would scan, or the error.

    Args:
        sql: A BigQuery standard SQL SELECT statement.
    """
    _log("🧪", "dry_run_query:", "yellow")
    console.print(Panel(sql.strip(), border_style="yellow", expand=False))
    if not _is_read_only(sql):
        return {"error": "Only read-only SELECT/WITH queries are allowed."}
    try:
        job = bq.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
                       location=config.BQ_LOCATION)
        _log("✓", f"   valid · would scan {job.total_bytes_processed:,} bytes", "green")
        return {"valid": True, "bytes_would_scan": job.total_bytes_processed}
    except Exception as e:  # noqa: BLE001
        return _query_error(e)


def execute_query(sql: str) -> dict:
    """Run a read-only BigQuery SQL query (hard byte cap applies) and return up to 50 rows.

    Args:
        sql: A BigQuery standard SQL SELECT statement. Must filter on the partition column.
    """
    _log("▶", "execute_query", "yellow")
    if not _is_read_only(sql):
        return {"error": "Only read-only SELECT/WITH queries are allowed."}
    try:
        cfg = bigquery.QueryJobConfig(maximum_bytes_billed=config.MAX_BYTES_BILLED)
        rows = [dict(row) for row in bq.query(sql, job_config=cfg, location=config.BQ_LOCATION).result(max_results=50)]
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
        _log("✓", f"   {len(rows)} row(s): {json.dumps(rows[:3], default=str)[:160]}", "green")
        return {"rows": rows}
    except Exception as e:  # noqa: BLE001
        return _query_error(e)


def reroute_traffic(from_gateway: str, to_gateway: str, reason: str) -> dict:
    """Move all checkout traffic away from a failing gateway to a healthy one. Reversible; expires in 30 min.

    Args:
        from_gateway: The failing gateway.
        to_gateway: The healthy gateway that should receive the traffic.
        reason: One sentence of evidence justifying the change.
    """
    _log("🔀", f"reroute_traffic({from_gateway!r} → {to_gateway!r})", "magenta")
    # Guardrails live in the tool, not the prompt.
    if from_gateway not in config.GATEWAYS or to_gateway not in config.GATEWAYS or from_gateway == to_gateway:
        return {"error": "invalid gateway pair"}
    target = r.get(f"gw_state:{to_gateway}")
    if target is None or json.loads(target)["error_rate"] >= 0.05:
        _log("✗", "   refused: target gateway is not verifiably healthy", "red")
        return {"error": f"refused: {to_gateway} is not verifiably healthy (needs live error_rate < 5%)"}
    if not r.set("routing:cooldown", "1", nx=True, ex=600):
        return {"error": "refused: a routing change happened in the last 10 minutes (anti-flapping cooldown)"}
    r.set("routing:disabled_gateway", from_gateway, ex=1800)
    r.set("routing:target_gateway", to_gateway, ex=1800)
    _log("✓", f"   traffic now flowing to {to_gateway}. Reason: {reason}", "green")
    return {"ok": True, "expires_in_minutes": 30}


def post_incident_report(title: str, report_markdown: str) -> dict:
    """Send the verified incident report to the on-call channel.

    Args:
        title: Short incident title.
        report_markdown: Markdown report: live evidence, baseline comparison with the SQL used, action taken, next steps.
    """
    _log("📣", f"post_incident_report({title!r})", "magenta")
    os.makedirs("incident_reports", exist_ok=True)
    path = f"incident_reports/{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.md"
    with open(path, "w") as f:
        f.write(f"# {title}\n\n{report_markdown}\n")
    console.print(Panel(Markdown(f"# {title}\n\n{report_markdown}"), border_style="magenta", title="on-call report"))
    return {"posted": True, "saved_to": path}


TOOLS = [get_live_gateway_state, describe_table, dry_run_query, execute_query, reroute_traffic, post_incident_report]

SYSTEM_GROUNDED = f"""You are an on-call payments reliability agent. You were woken by a streaming alert.
History lives in BigQuery table `{FULL_TABLE}`. Metric: failure_rate = SAFE_DIVIDE(failed_txns, total_txns).
Procedure:
1. Read the live state of the alerting gateway and of the other gateways.
2. describe_table, then write a baseline query: the alerting gateway's failure rate for the same hour_of_day
   over the last 30 days. Always dry_run_query before execute_query. Filter on the partition column.
3. Decide. It is a real outage only if the live error rate is far above baseline (e.g. >10x) with a large sample.
4. If real and a healthy alternative gateway exists (live error_rate < 5%), call reroute_traffic once.
   If no gateway is healthy, do not reroute; escalate in the report.
5. Always finish with post_incident_report containing the evidence, the SQL you used, and the action taken.
Never invent numbers. Every number in the report must come from a tool result."""

SYSTEM_UNGROUNDED = f"""You are an on-call payments reliability agent. History is in BigQuery table `{FULL_TABLE}`.
Check whether the alert is abnormal versus the last 30 days, act if needed, and post an incident report."""


def run_agent(alert: dict):
    STATE.update(t0=time.time(), query_errors=0)
    console.rule(f"[bold red]ALERT[/] {alert['gateway']} error_rate={alert['error_rate']:.1%} (window {alert['window_end']})")
    client = genai.Client()
    try:
        resp = _generate(client, alert)
    except errors.APIError as e:
        if getattr(e, "code", None) == 429:
            console.print(Panel(
                "Gemini quota hit (429 RESOURCE_EXHAUSTED). Each tool turn is a separate model request, "
                "so one investigation can make 6-10 requests in under a minute.\n\n"
                "Fix: wait ~60 s, clear the demo keys, and rerun:\n"
                "  docker exec reflex-redis redis-cli DEL routing:cooldown routing:disabled_gateway routing:target_gateway\n"
                "  python agent.py --once FastPay\n"
                "Longer term: enable billing on the AI Studio key, or use Vertex AI (see prep guide, step 6).",
                title="rate limited", border_style="red"))
            return
        raise
    _log("🏁", f"done. Final agent message: {(resp.text or '').strip()[:300]}", "bold green")


def _generate(client, alert: dict):
    return client.models.generate_content(
        model=config.GEMINI_MODEL,
        contents=f"Streaming alert received: {json.dumps(alert)}. Investigate and respond. "
                 f"Current UTC time: {datetime.now(timezone.utc).isoformat()}.",
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_GROUNDED if STATE["grounded"] else SYSTEM_UNGROUNDED,
            tools=TOOLS,
            temperature=0.1,
            automatic_function_calling=types.AutomaticFunctionCallingConfig(maximum_remote_calls=15),
        ),
    )


def listen():
    sub = pubsub_v1.SubscriberClient()
    path = sub.subscription_path(config.PROJECT_ID, config.ALERTS_SUB)

    def callback(msg):
        try:
            run_agent(json.loads(msg.data))
        finally:
            msg.ack()

    console.print(f"[bold]Agent listening on {config.ALERTS_SUB} ...[/]")
    future = sub.subscribe(path, callback=callback, flow_control=pubsub_v1.types.FlowControl(max_messages=1))
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", action="store_true")
    ap.add_argument("--once", metavar="GATEWAY")
    ap.add_argument("--no-grounding", action="store_true")
    a = ap.parse_args()
    STATE["grounded"] = not a.no_grounding
    if a.once:
        raw = r.get(f"gw_state:{a.once}")
        run_agent(json.loads(raw) if raw else {"gateway": a.once, "error_rate": 0.0, "window_end": "manual", "sample_size": 0})
    else:
        listen()
