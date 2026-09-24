"""Step 3: the reflex. Apache Beam streaming pipeline.

Pub/Sub events -> 60 s sliding windows every 5 s -> per-gateway summary
  -> latest summary written to Redis (the reflex store)
  -> alert published to Pub/Sub when a gateway breaches the threshold

Run locally (no Dataflow cost):
  python pipeline.py --runner=DirectRunner --streaming
"""
import json
import logging

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.utils.timestamp import Duration

import config


class ParseEvent(beam.DoFn):
    """Bad JSON goes to a dead-letter output instead of crashing the pipeline."""
    def process(self, msg: bytes):
        try:
            ev = json.loads(msg.decode("utf-8"))
            if ev.get("gateway") in config.GATEWAYS:
                yield ev
                return
            raise ValueError("unknown gateway")
        except Exception as e:  # noqa: BLE001
            yield beam.pvalue.TaggedOutput("dead_letter", {"raw": msg[:200], "error": str(e)})


class GatewayStats(beam.CombineFn):
    """Semantic compaction: thousands of events -> one small summary per gateway per window."""
    def create_accumulator(self):
        return {"n": 0, "errors": 0, "lat": [], "codes": {}}

    def add_input(self, acc, ev):
        acc["n"] += 1
        acc["lat"].append(float(ev["latency_ms"]))
        if ev["status"] != "SUCCESS":
            acc["errors"] += 1
            acc["codes"][ev["status"]] = acc["codes"].get(ev["status"], 0) + 1
        return acc

    def merge_accumulators(self, accs):
        out = self.create_accumulator()
        for a in accs:
            out["n"] += a["n"]; out["errors"] += a["errors"]; out["lat"].extend(a["lat"])
            for k, v in a["codes"].items():
                out["codes"][k] = out["codes"].get(k, 0) + v
        return out

    def extract_output(self, acc):
        lat = sorted(acc["lat"])
        p95 = lat[int(0.95 * (len(lat) - 1))] if lat else 0.0
        top = max(acc["codes"], key=acc["codes"].get) if acc["codes"] else None
        return {"sample_size": acc["n"], "error_rate": round(acc["errors"] / max(acc["n"], 1), 4),
                "top_error": top, "p95_latency_ms": round(p95, 1)}


class AddWindowInfo(beam.DoFn):
    def process(self, kv, win=beam.DoFn.WindowParam):
        gw, stats = kv
        yield {"window_end": win.end.to_utc_datetime().strftime("%Y-%m-%dT%H:%M:%SZ"), "gateway": gw, **stats}


class WriteReflexState(beam.DoFn):
    """Keep only the newest window per gateway. Idempotent: rewriting the same state is harmless."""
    def setup(self):
        import redis
        self.r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)

    def process(self, summary):
        key = f"gw_state:{summary['gateway']}"
        current = self.r.get(key)
        if current is None or json.loads(current)["window_end"] <= summary["window_end"]:
            self.r.set(key, json.dumps(summary), ex=300)  # TTL 5 min: stale state disappears
        yield summary


class PublishAlert(beam.DoFn):
    """Wake the agent. A Redis cooldown key makes duplicate alerts harmless."""
    def setup(self):
        import redis
        from google.cloud import pubsub_v1
        self.r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
        self.pub = pubsub_v1.PublisherClient()
        self.topic = self.pub.topic_path(config.PROJECT_ID, config.ALERTS_TOPIC)

    def process(self, summary):
        if self.r.set(f"alert_cooldown:{summary['gateway']}", "1", nx=True, ex=config.ALERT_COOLDOWN_S):
            self.pub.publish(self.topic, json.dumps(summary).encode()).result()
            logging.warning("ALERT %s error_rate=%.1f%% n=%d -> published to %s", summary["gateway"],
                            summary["error_rate"] * 100, summary["sample_size"], config.ALERTS_TOPIC)
        yield summary


def is_breach(s):
    return s["error_rate"] >= config.ERROR_RATE_THRESHOLD and s["sample_size"] >= config.MIN_SAMPLE_SIZE


def log_summary(s):
    flag = "  <-- BREACH" if is_breach(s) else ""
    logging.info("[window %s] %-9s n=%5d err=%6.2f%% p95=%7.1fms top=%s%s", s["window_end"], s["gateway"],
                 s["sample_size"], s["error_rate"] * 100, s["p95_latency_ms"], s["top_error"], flag)
    return s


def raise_direct_runner_pull_size(max_messages=1000):
    """The local DirectRunner pulls only 10 Pub/Sub messages per bundle (~5 events/s), far below
    the publisher's 200/s, so windows never reach MIN_SAMPLE_SIZE. Pull bigger batches instead.
    Only affects local runs; Dataflow uses its own Pub/Sub source."""
    from apache_beam.runners.direct import transform_evaluator as te
    original = te._PubSubReadEvaluator._get_subscriber_client.__func__

    class BigPull:
        def __init__(self, client):
            self._client = client

        def pull(self, **kwargs):
            kwargs["max_messages"] = max_messages
            return self._client.pull(**kwargs)

        def __getattr__(self, name):
            return getattr(self._client, name)

    te._PubSubReadEvaluator._get_subscriber_client = classmethod(lambda cls, t: BigPull(original(cls, t)))


def run(argv=None):
    raise_direct_runner_pull_size()
    opts = PipelineOptions(argv, save_main_session=True)
    opts.view_as(StandardOptions).streaming = True
    sub = f"projects/{config.PROJECT_ID}/subscriptions/{config.EVENTS_SUB}"

    with beam.Pipeline(options=opts) as p:
        parsed = (p
                  | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=sub, timestamp_attribute="event_ms")
                  | "Parse" >> beam.ParDo(ParseEvent()).with_outputs("dead_letter", main="events"))

        summaries = (parsed.events
                     | "SlidingWindow" >> beam.WindowInto(window.SlidingWindows(size=60, period=5),
                                                          allowed_lateness=Duration(seconds=120))
                     | "KeyByGateway" >> beam.Map(lambda ev: (ev["gateway"], ev))
                     | "Compact" >> beam.CombinePerKey(GatewayStats())
                     | "AddWindow" >> beam.ParDo(AddWindowInfo())
                     | "Log" >> beam.Map(log_summary))

        _ = summaries | "ReflexStore" >> beam.ParDo(WriteReflexState())
        _ = (summaries
             | "Breaches" >> beam.Filter(is_breach)
             | "Alert" >> beam.ParDo(PublishAlert()))
        _ = parsed.dead_letter | "DeadLetter" >> beam.Map(lambda d: logging.error("DEAD LETTER: %s", d))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
