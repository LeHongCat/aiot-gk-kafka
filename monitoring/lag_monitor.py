import argparse
import csv
import signal
import time

from confluent_kafka import Consumer, TopicPartition
from config.settings import KAFKA_COMMON, TOPIC, GROUP, PARTITIONS, output_dir


def observer(group):
    # No subscribe()/assign(): this observer never joins the processing group.
    return Consumer({**KAFKA_COMMON, "group.id": group, "enable.auto.commit": False})


def read_lag(client, topic, partitions=PARTITIONS):
    committed = client.committed([TopicPartition(topic, p) for p in range(partitions)], timeout=5)
    rows = []
    for part in committed:
        _, high = client.get_watermark_offsets(TopicPartition(topic, part.partition), timeout=5, cached=False)
        # Unknown offsets are unknown lag, not zero lag.
        offset = part.offset if part.offset >= 0 and not part.error else None
        rows.append({"partition": part.partition, "log_end_offset": high, "committed_offset": offset,
                     "lag": max(0, high - offset) if offset is not None else None})
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--group", default=GROUP)
    parser.add_argument("--run-id", default="demo01")
    parser.add_argument("--interval", type=float, default=1)
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("interval must be positive")
    out = output_dir(args.run_id)
    path = out / "lag.csv"
    client = observer(args.group)
    stop = False

    def handler(signum, frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    try:
        with path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["epoch", "partition", "log_end_offset", "committed_offset", "lag", "group_lag"])
            while not stop and not (out / "stop.requested").exists():
                try:
                    rows = read_lag(client, args.topic)
                    total = sum(r["lag"] for r in rows) if all(r["lag"] is not None for r in rows) else None
                    stamp = time.time()
                    for row in rows:
                        writer.writerow([stamp, row["partition"], row["log_end_offset"], row["committed_offset"], row["lag"], total])
                    f.flush()
                    print(f"group_lag={total if total is not None else 'unknown'}", flush=True)
                except Exception as exc:
                    print(f"lag sample failed: {exc}", flush=True)
                time.sleep(args.interval)
    finally:
        client.close()


if __name__ == "__main__":
    main()
