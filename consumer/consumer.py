"""Consumer of one group: validates events, counts per second, logs rebalances
and records which sensor came from which partition (proof of sensor -> 1 consumer)."""
import argparse
import csv
import json
import os
import signal
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime

from confluent_kafka import Consumer, KafkaException
from config.settings import KAFKA_COMMON, TOPIC, GROUP, output_dir
from consumer.validation import validate_event

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="consumer name, e.g. C1")
    parser.add_argument("--run-id", default="demo01")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default=GROUP)
    parser.add_argument("--delay-ms", type=float, default=0, help="simulated slow processing per message")
    parser.add_argument("--verbose", action="store_true", help="print every message: sensor -> partition -> this consumer")
    args = parser.parse_args()
    out = output_dir(args.run_id)
    session = time.strftime("%Y%m%d-%H%M%S")
    tag = f"{args.id}_{session}"

    c = Consumer({
        **KAFKA_COMMON,
        "group.id": args.group,
        "client.id": f"{args.id}-{args.run_id}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "enable.auto.offset.store": False,
    })
    rebalance_log = (out / f"rebalance_{tag}.jsonl").open("a", encoding="utf-8")

    def log_rebalance(kind, parts):
        row = {"epoch": time.time(), "consumer_id": args.id, "event": kind, "partitions": sorted(p.partition for p in parts)}
        rebalance_log.write(json.dumps(row) + "\n")
        rebalance_log.flush()
        print(f"[{args.id}] {kind.upper():<6} partitions={row['partitions']}", flush=True)
        return row

    def on_assign(cons, parts):
        row = log_rebalance("assign", parts)
        (out / f"ready_{args.id}.json").write_text(json.dumps({"pid": os.getpid(), **row}), encoding="utf-8")

    c.subscribe([args.topic], on_assign=on_assign,
                on_revoke=lambda cons, parts: log_rebalance("revoke", parts),
                on_lost=lambda cons, parts: log_rebalance("lost", parts))

    stop = False

    def handler(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    if hasattr(signal, "SIGBREAK"):  # Windows: CTRL_BREAK_EVENT is how demo_rebalance asks a consumer to stop
        signal.signal(signal.SIGBREAK, handler)

    sensor_parts = defaultdict(Counter)
    invalid_f = (out / f"invalid_{tag}.jsonl").open("a", encoding="utf-8")
    # Offsets seen per partition as contiguous runs [first, last], plus count of re-reads
    # (offset <= highest seen), so monitoring.verify can prove no loss / no duplicates.
    runs = defaultdict(list)
    highest = {}
    reads = Counter()
    replayed = Counter()
    total = 0
    cur_sec, processed, invalid = int(time.time()), 0, 0
    delay = args.delay_ms / 1000

    with (out / f"consumer_metrics_{tag}.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["epoch_second", "consumer_id", "processed", "invalid", "total_processed"])

        def flush_second():
            nonlocal processed, invalid
            if processed or invalid:
                writer.writerow([cur_sec, args.id, processed, invalid, total])
                f.flush()
            processed = invalid = 0

        print(f"[{args.id}] started, waiting for partitions of '{args.topic}' (group '{args.group}')", flush=True)
        try:
            next_stop_check = 0.0
            while not stop:
                # Touching the filesystem on every poll is slow on some drives; once a second is enough.
                if time.monotonic() >= next_stop_check:
                    if (out / "stop.requested").exists():
                        break
                    next_stop_check = time.monotonic() + 1
                msgs = c.consume(num_messages=500, timeout=0.2)
                last = {}
                for msg in msgs:
                    if msg.error():
                        print(f"[{args.id}] error: {msg.error()}", flush=True)
                        continue
                    now = int(time.time())
                    if now != cur_sec:
                        flush_second()
                        cur_sec = now
                    part, off = msg.partition(), msg.offset()
                    reads[part] += 1
                    if off <= highest.get(part, -1):
                        replayed[part] += 1
                    highest[part] = max(highest.get(part, -1), off)
                    if runs[part] and runs[part][-1][1] + 1 == off:
                        runs[part][-1][1] = off
                    else:
                        runs[part].append([off, off])
                    try:
                        event = json.loads(msg.value())
                        validate_event(event)
                    except Exception as exc:
                        invalid += 1
                        invalid_f.write(json.dumps({"partition": msg.partition(), "offset": msg.offset(),
                                                    "error": str(exc), "raw": msg.value().decode("utf-8", "replace")}) + "\n")
                    else:
                        processed += 1
                        total += 1
                        sensor_parts[event["sensor_id"]][msg.partition()] += 1
                        if args.verbose:
                            try:
                                sent_at = datetime.fromisoformat(event["timestamp"][:-1] + "+00:00").timestamp()
                                lat = f"latency {(time.time() - sent_at) * 1000:.0f} ms"
                            except ValueError:
                                lat = ""
                            print(f"[{args.id}] RECEIVED {event['sensor_id']:<10} <-- partition={msg.partition()} "
                                  f"offset={msg.offset()}  {msg.value().decode()}  {lat}", flush=True)
                        if delay:
                            time.sleep(delay)
                    last[msg.partition()] = msg
                for m in last.values():
                    c.store_offsets(message=m)
        finally:
            flush_second()
            invalid_f.close()
            try:
                c.commit(asynchronous=False)
            except KafkaException:
                pass
            c.close()
            rebalance_log.close()
            (out / f"sensor_map_{tag}.json").write_text(json.dumps(
                {"consumer": args.id, "sensors": {s: dict(p) for s, p in sorted(sensor_parts.items())}}, indent=2), encoding="utf-8")
            (out / f"offsets_{tag}.json").write_text(json.dumps({
                "consumer": args.id,
                "partitions": {str(p): {"reads": reads[p], "replayed": replayed[p], "runs": runs[p]} for p in sorted(runs)}}),
                encoding="utf-8")
            print(f"[{args.id}] stopped, processed={total}", flush=True)


if __name__ == "__main__":
    main()
