"""Visual demo: one sensor -> always the same partition -> always the same consumer.

Runs a small live experiment (few sensors, few messages, slow enough to read):
  producer --key=sensor_id--> partition --(group assigns 1 partition to 1 consumer)--> consumer
Use --no-key to see the contrast: without a key, one sensor is spread over many consumers.
"""
import argparse
import json
import queue
import random
import sys
import threading
import time
from collections import Counter, defaultdict

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_COMMON, output_dir
from producer.sensor_generator import generate_sensor

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

COLORS = ["\033[36m", "\033[33m", "\033[35m", "\033[32m", "\033[34m", "\033[31m"]
RESET, BOLD, DIM = "\033[0m", "\033[1m", "\033[2m"


def color(i, text):
    return f"{COLORS[i % len(COLORS)]}{text}{RESET}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="routing01")
    parser.add_argument("--sensors", type=int, default=6, help="number of simulated sensors")
    parser.add_argument("--messages", type=int, default=36, help="total messages to send")
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--interval", type=float, default=0.25, help="seconds between messages (slow = readable)")
    parser.add_argument("--no-key", action="store_true", help="send WITHOUT key to show the contrast")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    if min(args.sensors, args.messages, args.consumers, args.partitions) < 1:
        parser.error("all counts must be >= 1")
    if args.interval < 0:
        parser.error("--interval must be >= 0")

    import os
    os.system("")  # enable ANSI colors in Windows terminals
    out = output_dir(args.run_id)
    stamp = int(time.time())
    topic, group = f"routing-{args.run_id}-{stamp}", f"routing-{args.run_id}-{stamp}"
    admin = AdminClient({**KAFKA_COMMON})
    for name, fut in admin.create_topics([NewTopic(topic, args.partitions, 1)]).items():
        fut.result(15)

    mode = "NO key (messages spread across partitions)" if args.no_key else "key = sensor_id"
    print(f"\n{BOLD}=== 1:1 DEMO  sensor -> partition -> consumer ==={RESET}")
    print(f"Topic {topic} | {args.partitions} partitions | {args.consumers} consumers in one group | mode: {mode}\n")

    events = queue.Queue()
    stop = threading.Event()
    assigned = {}
    lock = threading.Lock()

    def consumer_thread(idx):
        cid = f"C{idx + 1}"
        c = Consumer({**KAFKA_COMMON, "group.id": group, "client.id": cid,
                      "auto.offset.reset": "earliest", "enable.auto.commit": False})

        def on_assign(cons, parts):
            with lock:
                assigned[cid] = sorted(p.partition for p in parts)

        def on_revoke(cons, parts):
            with lock:
                assigned[cid] = []

        c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
        while not stop.is_set():
            msg = c.poll(0.1)
            if msg is not None and not msg.error():
                events.put(("recv", cid, msg.partition(), json.loads(msg.value())["sensor_id"]))
        c.close()

    threads = [threading.Thread(target=consumer_thread, args=(i,), daemon=True) for i in range(args.consumers)]
    for t in threads:
        t.start()

    print("Waiting for Kafka to assign partitions to the consumers...")
    deadline = time.time() + 60
    while time.time() < deadline:
        with lock:
            if sum(len(v) for v in assigned.values()) == args.partitions:
                break
        time.sleep(0.3)
    else:
        stop.set()
        raise SystemExit("Timeout: the group did not finish assigning partitions. Check `docker compose ps`.")
    time.sleep(1)
    with lock:
        owner = {p: cid for cid, ps in assigned.items() for p in ps}
    print(f"{BOLD}Kafka assigned the partitions (each partition belongs to exactly 1 consumer):{RESET}")
    for p in sorted(owner):
        cid = owner[p]
        print(f"   Partition {p}  ==>  {color(int(cid[1:]) - 1, 'Consumer ' + cid)}")
    print(f"\n{BOLD}Sending now. Each SEND line is one request from one sensor:{RESET}\n")

    producer = Producer({**KAFKA_COMMON, "linger.ms": 0})
    rng = random.Random(args.seed)
    sent = Counter()
    sensor_partition = defaultdict(Counter)

    def delivered(err, msg):
        if err is None:
            events.put(("sent", None, msg.partition(), msg.key().decode() if msg.key() else None))

    printer_done = threading.Event()
    got = Counter()
    matrix = defaultdict(Counter)
    seen_partitions = defaultdict(set)

    def printer():
        pending_sent = {}
        while not printer_done.is_set() or not events.empty():
            try:
                kind, cid, part, sensor = events.get(timeout=0.2)
            except queue.Empty:
                continue
            if kind == "sent":
                sensor_partition[sensor][part] += 1
                print(f"{DIM}SEND{RESET}  {sensor or '(no key)':<10} --> Partition {part}")
            else:
                matrix[sensor][cid] += 1
                seen_partitions[sensor].add(part)
                got[cid] += 1
                i = int(cid[1:]) - 1
                print(f"         Partition {part} --> {color(i, cid)}  processed {color(i, sensor)}")

    pt = threading.Thread(target=printer, daemon=True)
    pt.start()
    for _ in range(args.messages):
        event = generate_sensor(rng, args.sensors)
        key = None if args.no_key else event["sensor_id"].encode()
        producer.produce(topic, key=key, value=json.dumps(event).encode(), on_delivery=delivered)
        producer.flush(10)
        sent[event["sensor_id"]] += 1
        time.sleep(args.interval)

    deadline = time.time() + 20
    while sum(got.values()) < args.messages and time.time() < deadline:
        time.sleep(0.2)
    time.sleep(0.5)
    printer_done.set()
    pt.join(5)
    stop.set()
    for t in threads:
        t.join(5)

    cids = [f"C{i + 1}" for i in range(args.consumers)]
    print(f"\n{BOLD}=== RESULT: which consumer processed each sensor? (message counts) ==={RESET}")
    print(f"{'Sensor':<11}{'Partition':<12}" + "".join(f"{c:>6}" for c in cids) + "   Verdict")
    all_one = True
    for sensor in sorted(matrix):
        row = matrix[sensor]
        who = [c for c in cids if row[c]]
        ok = len(who) == 1
        all_one &= ok
        cells = "".join(color(int(c[1:]) - 1, f"{row[c]:>6}") if row[c] else f"{'.':>6}" for c in cids)
        parts = ",".join(map(str, sorted(seen_partitions[sensor])))
        verdict = f"\033[32m1:1 -> {who[0]}{RESET}" if ok else f"\033[31msplit across {len(who)} consumers{RESET}"
        print(f"{sensor:<11}{parts:<12}{cells}   {verdict}")
    print()
    if all_one:
        print(f"{BOLD}\033[32mPASS: every sensor was processed by exactly 1 consumer.{RESET}")
        print("Why: key = sensor_id -> hash -> always the same partition; a partition has only 1 consumer in the group.")
    else:
        print(f"{BOLD}\033[31mNOT 1:1: the same sensor was processed by several consumers.{RESET}")
        print("Why: without a key Kafka spreads messages over partitions, so per-sensor routing and ordering are lost.")
    print(f"Total: sent {sum(sent.values())}, received {sum(got.values())}. "
          f"Several sensors may share one consumer (many:1), but one sensor is never split across consumers.")

    result = {"topic": topic, "keyed": not args.no_key, "partitions": args.partitions,
              "partition_owner": {str(p): c for p, c in owner.items()},
              "sensor_to_consumer": {s: dict(r) for s, r in matrix.items()},
              "sensor_to_partitions": {s: sorted(p) for s, p in seen_partitions.items()},
              "one_to_one": all_one, "sent": sum(sent.values()), "received": sum(got.values())}
    suffix = "nokey" if args.no_key else "key"
    (out / f"routing_{suffix}.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        sensors = sorted(matrix)
        data = [[matrix[s][c] for c in cids] for s in sensors]
        fig, ax = plt.subplots(figsize=(1.2 * len(cids) + 3, 0.5 * len(sensors) + 2))
        ax.imshow(data, cmap="Blues")
        ax.set_xticks(range(len(cids)), cids)
        ax.set_yticks(range(len(sensors)), sensors)
        for y, row in enumerate(data):
            for x, v in enumerate(row):
                ax.text(x, y, v or "", ha="center", va="center")
        ax.set_title("Sensor -> Consumer " + ("(keyed: 1:1)" if not args.no_key else "(no key)"))
        fig.tight_layout()
        fig.savefig(out / f"routing_{suffix}.png", dpi=150)
        plt.close(fig)
    except Exception as exc:
        print(f"(chart skipped: {exc})")
    print(f"\nSaved: output/{args.run_id}/routing_{suffix}.json and routing_{suffix}.png")
    try:
        admin.delete_topics([topic])
    except Exception:
        pass
    # With key, 1:1 is required. Without key it is only a contrast demo.
    return 0 if args.no_key or all_one else 1


if __name__ == "__main__":
    raise SystemExit(main())
