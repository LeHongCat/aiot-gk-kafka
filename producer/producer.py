"""Async producer with pacing, broker delivery counters and one-second bins."""
import argparse
import csv
import json
import random
import time
from collections import Counter

from confluent_kafka import Producer
from config.settings import KAFKA_COMMON, TOPIC, RATE, DURATION, NUM_SENSORS, output_dir
from producer.sensor_generator import generate_sensor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=RATE)
    parser.add_argument("--duration", type=float, default=DURATION)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--run-id", default="demo01")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--verbose", action="store_true",
                        help="print every message with the partition/offset the broker assigned (use a low --rate)")
    parser.add_argument("--spike-rate", type=int, default=0, help="burst rate (msg/s) used during the spike window")
    parser.add_argument("--spike-start", type=float, default=0, help="seconds after start when the spike begins")
    parser.add_argument("--spike-duration", type=float, default=0, help="length of the spike in seconds")
    args = parser.parse_args()
    if args.rate <= 0 or args.duration <= 0 or NUM_SENSORS <= 0:
        parser.error("rate, duration and NUM_SENSORS must be positive")
    if args.spike_rate and (args.spike_rate < args.rate or args.spike_duration <= 0 or args.spike_start < 0
                            or args.spike_start + args.spike_duration > args.duration):
        parser.error("spike must have spike-rate >= rate, spike-duration > 0 and fit inside --duration")

    def cumulative(t):
        """Messages that should have been sent by time t: base rate plus the extra burst volume."""
        extra = 0.0
        if args.spike_rate:
            extra = (args.spike_rate - args.rate) * min(max(t - args.spike_start, 0.0), args.spike_duration)
        return args.rate * t + extra
    out = output_dir(args.run_id)
    stats_path = out / "producer_stats.json"
    # Prevent mixing producer runs. Automated benchmarks also isolate topics.
    lock_path = out / "producer.started"
    with lock_path.open("x", encoding="utf-8") as f:
        f.write(str(time.time()))
    producer = Producer({
        **KAFKA_COMMON,
        "client.id": f"producer-{args.run_id}",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
        "delivery.timeout.ms": 30000,
    })
    counts = Counter()
    buckets = Counter()
    rng = random.Random(args.seed)

    def delivered(err, msg):
        if err is not None:
            counts["delivery_errors"] += 1
            if args.verbose:
                print(f"SEND FAILED key={msg.key().decode()} error={err}", flush=True)
        else:
            counts["delivered"] += 1
            buckets[int(time.time())] += 1
            if args.verbose:
                print(f"SENT  {msg.key().decode():<10} --> topic={msg.topic()} partition={msg.partition()} "
                      f"offset={msg.offset()}  {msg.value().decode()}", flush=True)

    start_wall = time.time()
    start = time.perf_counter()
    deadline = start + args.duration
    planned = int(cumulative(args.duration))
    next_log = start + 1
    try:
        while time.perf_counter() < deadline and counts["enqueued"] < planned:
            now = time.perf_counter()
            due = min(planned, int(cumulative(now - start)) + 1)
            batch_n = min(500, max(0, due - counts["enqueued"]))
            for _ in range(batch_n):
                if time.perf_counter() >= deadline:
                    break
                event = generate_sensor(rng, NUM_SENSORS)
                value = json.dumps(event, separators=(",", ":"), allow_nan=False).encode()
                try:
                    producer.produce(args.topic, key=event["sensor_id"].encode(), value=value, on_delivery=delivered)
                    counts["enqueued"] += 1
                except BufferError:
                    counts["queue_full_events"] += 1
                    producer.poll(0.01)
                    break
            producer.poll(0)
            if batch_n == 0:
                time.sleep(min(0.005, 1 / args.rate))
            if now >= next_log:
                print(f"enqueued={counts['enqueued']} delivered={counts['delivered']} errors={counts['delivery_errors']}", flush=True)
                next_log = now + 1
    except KeyboardInterrupt:
        counts["interrupted"] = 1
    send_end_wall = time.time()
    send_end = time.perf_counter()
    remaining = producer.flush(35)
    end = time.perf_counter()
    stats = {
        "run_id": args.run_id, "topic": args.topic, "target_rate": args.rate,
        "requested_duration_s": args.duration, "planned_messages": planned,
        "spike_rate": args.spike_rate, "spike_start_s": args.spike_start, "spike_duration_s": args.spike_duration,
        "enqueued": counts["enqueued"], "delivered": counts["delivered"],
        "delivery_errors": counts["delivery_errors"], "undelivered": remaining,
        "queue_full_events": counts["queue_full_events"],
        "not_enqueued": planned - counts["enqueued"],
        "interrupted": bool(counts["interrupted"]),
        "start_epoch": start_wall, "send_end_epoch": send_end_wall,
        "send_window_s": send_end - start, "flush_s": end - send_end,
        "duration_including_flush_s": end - start,
        "delivery_rate_including_flush": counts["delivered"] / (end - start),
    }
    stats_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    with (out / "producer_metrics.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["epoch_second", "delivered"])
        writer.writerows(sorted(buckets.items()))
    print(json.dumps(stats, indent=2), flush=True)
    return 1 if remaining or counts["delivery_errors"] or counts["interrupted"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
