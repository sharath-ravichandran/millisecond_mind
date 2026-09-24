"""Step 1: create the BigQuery 'history' table and load 30 days of synthetic hourly stats.

Load jobs are free; the table is ~2,200 rows, so this costs nothing.
Run:  python generate_history.py
"""
import random
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery

import config

SCHEMA = [
    bigquery.SchemaField("hour_ts", "TIMESTAMP", mode="REQUIRED",
        description="Start of the hour in UTC. Partition key: every query must filter on it."),
    bigquery.SchemaField("gateway", "STRING", mode="REQUIRED",
        description="Payment gateway name. Allowed values: 'FastPay', 'SecurePay', 'QuickPay'."),
    bigquery.SchemaField("day_of_week", "STRING",
        description="Day name of hour_ts in UTC, e.g. 'Tuesday'."),
    bigquery.SchemaField("hour_of_day", "INT64",
        description="Hour of day of hour_ts in UTC, 0-23."),
    bigquery.SchemaField("total_txns", "INT64",
        description="Number of payment attempts routed to this gateway in the hour."),
    bigquery.SchemaField("failed_txns", "INT64",
        description="Payment attempts that did not end in SUCCESS. "
                    "Metric definition: failure_rate = SAFE_DIVIDE(failed_txns, total_txns)."),
    bigquery.SchemaField("p95_latency_ms", "FLOAT64",
        description="95th percentile gateway response latency in milliseconds for the hour."),
]

BASE_SHARE = {"FastPay": 0.6, "SecurePay": 0.3, "QuickPay": 0.1}
BASE_FAIL = {"FastPay": 0.008, "SecurePay": 0.006, "QuickPay": 0.012}
BASE_P95 = {"FastPay": 420.0, "SecurePay": 510.0, "QuickPay": 640.0}


def hourly_volume(ts: datetime) -> int:
    # Busy daytime (IST ~ UTC+5:30), quiet nights, weekly flash sale Tuesday 08:00-10:00 UTC.
    ist_hour = (ts + timedelta(hours=5, minutes=30)).hour
    curve = 0.25 + 0.75 * max(0.0, 1 - abs(ist_hour - 14) / 10)
    vol = 60000 * curve
    if ts.strftime("%A") == "Tuesday" and ts.hour in (8, 9):
        vol *= 3
    return int(vol * random.uniform(0.9, 1.1))


def build_rows():
    random.seed(42)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = now - timedelta(days=30)
    incident_hour = start + timedelta(days=17, hours=6)  # a past QuickPay incident, for realism
    rows, ts = [], start
    while ts < now:
        vol = hourly_volume(ts)
        flash = ts.strftime("%A") == "Tuesday" and ts.hour in (8, 9)
        for gw in config.GATEWAYS:
            total = int(vol * BASE_SHARE[gw])
            fail = BASE_FAIL[gw] * (1.4 if flash else 1.0) * random.uniform(0.8, 1.2)
            p95 = BASE_P95[gw] * (1.3 if flash else 1.0) * random.uniform(0.9, 1.1)
            if gw == "QuickPay" and ts == incident_hour:
                fail, p95 = 0.35, 3900.0
            rows.append({
                "hour_ts": ts.isoformat(), "gateway": gw, "day_of_week": ts.strftime("%A"),
                "hour_of_day": ts.hour, "total_txns": total, "failed_txns": int(total * fail),
                "p95_latency_ms": round(p95, 1),
            })
        ts += timedelta(hours=1)
    return rows


def main():
    client = bigquery.Client(project=config.PROJECT_ID)
    dataset_id = f"{config.PROJECT_ID}.{config.DATASET}"
    ds = bigquery.Dataset(dataset_id)
    ds.location = config.BQ_LOCATION
    client.create_dataset(ds, exists_ok=True)

    table = bigquery.Table(f"{dataset_id}.{config.TABLE}", schema=SCHEMA)
    table.description = ("Hourly payment outcomes per gateway. Use for baselines: compare a live "
                         "error rate with the same hour_of_day over recent days.")
    table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="hour_ts")
    table.require_partition_filter = True  # guardrail: no accidental full scans
    client.delete_table(table, not_found_ok=True)
    client.create_table(table)

    rows = build_rows()
    job = client.load_table_from_json(
        rows, table, job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE"))
    job.result()
    print(f"Loaded {len(rows)} rows into {table.full_table_id}")


if __name__ == "__main__":
    main()
