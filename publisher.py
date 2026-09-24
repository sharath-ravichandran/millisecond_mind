"""Step 2: publish synthetic checkout events to Pub/Sub, and optionally inject a FastPay outage.

Run:  python publisher.py --rate 200 --fail-after 90
It also reads the routing override the agent writes to Redis, so after the agent
reroutes, you'll see overall checkout failures drop. That's your "recovery" screenshot.
"""
import argparse
import json
import random
import time
import uuid

import redis
from google.cloud import pubsub_v1
from rich.console import Console

import config

console = Console()
SHARE = {"FastPay": 0.6, "SecurePay": 0.3, "QuickPay": 0.1}
BASE_FAIL = {"FastPay": 0.008, "SecurePay": 0.006, "QuickPay": 0.012}


def pick_gateway(disabled: str | None, target: str | None) -> str:
    share = dict(SHARE)
    if disabled in share and target in share and target != disabled:
        share[target] += share.pop(disabled)  # agent's reroute: move that traffic to the target
    r, acc = random.random() * sum(share.values()), 0.0
    for gw, s in share.items():
        acc += s
        if r <= acc:
            return gw
    return "SecurePay"


def make_event(gw: str, outage: bool) -> tuple[dict, int]:
    fail_p = 0.78 if (outage and gw == "FastPay") else BASE_FAIL[gw]
    failed = random.random() < fail_p
    status = random.choice(["504_TIMEOUT"] * 8 + ["502_BAD_GATEWAY"] * 2) if failed else "SUCCESS"
    latency = random.uniform(3500, 6000) if (failed and status == "504_TIMEOUT") else random.gauss(300, 80)
    now_ms = int(time.time() * 1000)
    # ~1% of events arrive "late": their event time is 60-90 s in the past (think flaky mobile networks).
    event_ms = now_ms - random.randint(60_000, 90_000) if random.random() < 0.01 else now_ms
    ev = {"event_id": str(uuid.uuid4()), "gateway": gw, "status": status,
          "latency_ms": round(max(latency, 20), 1), "event_ms": event_ms}
    return ev, event_ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=200, help="events per second")
    ap.add_argument("--fail-after", type=int, default=90, help="seconds before FastPay outage starts (-1 = never)")
    ap.add_argument("--duration", type=int, default=900, help="seconds to run")
    args = ap.parse_args()

    publisher = pubsub_v1.PublisherClient(
        batch_settings=pubsub_v1.types.BatchSettings(max_messages=500, max_latency=0.05))
    topic = publisher.topic_path(config.PROJECT_ID, config.EVENTS_TOPIC)
    r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
    r.delete("routing:disabled_gateway", "routing:target_gateway")

    start = time.time()
    console.print(f"[bold cyan]Publishing ~{args.rate} events/s to {config.EVENTS_TOPIC}[/]")
    while time.time() - start < args.duration:
        sec_start = time.time()
        outage = args.fail_after >= 0 and (sec_start - start) >= args.fail_after
        disabled, target = r.get("routing:disabled_gateway"), r.get("routing:target_gateway")
        counts, fails = {}, {}
        for _ in range(args.rate):
            gw = pick_gateway(disabled, target)
            ev, event_ms = make_event(gw, outage)
            publisher.publish(topic, json.dumps(ev).encode(), event_ms=str(event_ms))
            counts[gw] = counts.get(gw, 0) + 1
            fails[gw] = fails.get(gw, 0) + (ev["status"] != "SUCCESS")
        total_fail = sum(fails.values()) / max(sum(counts.values()), 1)
        per_gw = "  ".join(f"{g}: {counts.get(g,0):>3} ({fails.get(g,0)/max(counts.get(g,0),1):>5.1%})" for g in config.GATEWAYS)
        colour = "red" if total_fail > 0.05 else "green"
        tag = " [bold red]OUTAGE INJECTED[/]" if outage else ""
        route = f" [yellow]routing: {disabled} -> {target}[/]" if disabled else ""
        console.print(f"t+{int(sec_start-start):>4}s  {per_gw}  |  checkout failures [{colour}]{total_fail:6.1%}[/]{tag}{route}")
        time.sleep(max(0.0, 1.0 - (time.time() - sec_start)))


if __name__ == "__main__":
    main()
