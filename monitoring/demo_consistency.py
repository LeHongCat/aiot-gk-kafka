"""Visual demo: every sensor always lands on the same partition and the same consumer, round after round.

Each round the producer pushes ONE reading from EVERY sensor. For each round you see which consumer
(and partition) received which sensors; the layout must be identical in every round. At the end:
the hash table (sensor -> crc32 -> partition -> consumer), a sensor x round matrix, and the load per partition.

  python -m monitoring.demo_consistency                    # 30 sensors, 5 rounds, track motor_001
  python -m monitoring.demo_consistency --sensors 100      # bigger population: see the split over 3 partitions
  python -m monitoring.demo_consistency --track motor_007 --rounds 8
  python -m monitoring.demo_consistency --step             # press Enter between rounds
"""
import argparse
import json
import random
import sys
import threading
import time
import zlib
from collections import Counter, defaultdict

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_COMMON, output_dir
from producer.sensor_generator import generate_sensor

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

COLORS = ["\033[36m", "\033[33m", "\033[35m", "\033[32m", "\033[34m", "\033[31m"]
RESET, BOLD, DIM, RED, GREEN = "\033[0m", "\033[1m", "\033[2m", "\033[31m", "\033[32m"


def paint(cid, text):
    return f"{COLORS[(int(cid[1:]) - 1) % len(COLORS)]}{text}{RESET}"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default="consistency01")
    parser.add_argument("--sensors", type=int, default=30)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--track", default="motor_001", help="sensor highlighted in every round")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between rounds (ignored with --step)")
    parser.add_argument("--step", action="store_true", help="wait for Enter before each round")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    if min(args.sensors, args.rounds, args.consumers, args.partitions) < 1 or args.interval < 0:
        parser.error("counts must be >= 1 and --interval >= 0")
    sensors = [f"motor_{i:03d}" for i in range(1, args.sensors + 1)]
    if args.track not in sensors:
        parser.error(f"--track must be one of motor_001..motor_{args.sensors:03d}")

    import os
    os.system("")  # enable ANSI colors in Windows terminals
    out = output_dir(args.run_id)
    topic = group = f"consistency-{args.run_id}-{int(time.time())}"
    admin = AdminClient({**KAFKA_COMMON})  # keep the reference until the futures complete
    for fut in admin.create_topics([NewTopic(topic, args.partitions, 1)]).values():
        fut.result(15)

    assigned, arrivals, lock, stop = {}, [], threading.Lock(), threading.Event()

    def consumer_thread(idx):
        cid = f"C{idx + 1}"
        c = Consumer({**KAFKA_COMMON, "group.id": group, "client.id": cid, "auto.offset.reset": "earliest",
                      "enable.auto.commit": False})

        def on_assign(cons, parts):
            with lock:
                assigned[cid] = sorted(p.partition for p in parts)

        def on_revoke(cons, parts):
            with lock:
                assigned[cid] = []

        c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
        while not stop.is_set():
            msg = c.poll(0.05)
            if msg is not None and not msg.error():
                with lock:
                    arrivals.append((cid, msg.partition(), json.loads(msg.value())["sensor_id"], json.loads(msg.value())["round"]))
        c.close()

    threads = [threading.Thread(target=consumer_thread, args=(i,), daemon=True) for i in range(args.consumers)]
    for t in threads:
        t.start()

    print(f"\n{BOLD}=== CONSISTENCY DEMO: same sensor -> same partition -> same consumer, every round ==={RESET}")
    print(f"Topic {topic} | {args.partitions} partitions | {args.consumers} consumers (1 group) | "
          f"{args.sensors} sensors | {args.rounds} rounds | key = sensor_id")
    print("Waiting for Kafka to assign partitions...")
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
    print(f"\n{BOLD}Partition ownership (set by Kafka, fixed while the group is stable):{RESET}")
    for p in sorted(owner):
        print(f"   Partition {p}  ==>  {paint(owner[p], 'Consumer ' + owner[p])}")
    t = args.track
    crc = zlib.crc32(t.encode())
    print(f"\n{BOLD}How '{t}' is routed:{RESET} crc32('{t}') = {crc};  {crc} % {args.partitions} = {crc % args.partitions}"
          f"  ->  Partition {crc % args.partitions}  ->  {paint(owner[crc % args.partitions], owner[crc % args.partitions])}")

    producer = Producer({**KAFKA_COMMON, "linger.ms": 5})
    rng = random.Random(args.seed)
    placed = {}                      # (round, sensor) -> (partition, consumer)
    ack = {}                         # (round, sensor) -> partition from the broker ACK
    sent_total = 0

    def delivered(err, msg):
        if err is None:
            ev = json.loads(msg.value())
            ack[(ev["round"], ev["sensor_id"])] = msg.partition()

    base = None
    try:
        for rnd in range(1, args.rounds + 1):
            if args.step:
                try:
                    input(f"\n{DIM}[Enter] push round {rnd}/{args.rounds} (Ctrl+C to stop){RESET}")
                except EOFError:
                    args.step = False
            for s in sensors:
                event = generate_sensor(rng, args.sensors)
                event.update(sensor_id=s, round=rnd)
                producer.produce(topic, key=s.encode(), value=json.dumps(event).encode(), on_delivery=delivered)
            producer.flush(15)
            sent_total += len(sensors)
            end = time.time() + 10
            while time.time() < end:
                with lock:
                    if sum(1 for a in arrivals if a[3] == rnd) >= len(sensors):
                        break
                time.sleep(0.05)
            with lock:
                got = [a for a in arrivals if a[3] == rnd]
            layout = defaultdict(list)
            for cid, p, s, _ in got:
                placed[(rnd, s)] = (p, cid)
                layout[(cid, p)].append(s)
            current = {k: sorted(v) for k, v in layout.items()}
            if base is None:
                base = current
            same = current == base
            print(f"\n{BOLD}ROUND {rnd}{RESET}  pushed {len(sensors)} readings (1 per sensor), received {len(got)}")
            for (cid, p) in sorted(layout, key=lambda k: k[1]):
                names = " ".join(s[-3:] for s in sorted(layout[(cid, p)]))
                bar = "#" * len(layout[(cid, p)])
                print(f"   {paint(cid, cid)} <- P{p} [{len(layout[(cid, p)]):>3}] {paint(cid, bar)}  {DIM}{names}{RESET}")
            tp = placed.get((rnd, t))
            print(f"   {BOLD}{t}{RESET} -> " + (f"Partition {tp[0]} -> {paint(tp[1], tp[1])}" if tp else f"{RED}not received{RESET}")
                  + f"      layout identical to round 1: "
                  + (f"{GREEN}YES{RESET}" if same else f"{RED}NO{RESET}"))
            if not args.step and rnd < args.rounds:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    stop.set()
    for th in threads:
        th.join(5)

    rounds_done = sorted({r for r, _ in placed})
    print(f"\n{BOLD}=== MATRIX: where did each sensor land, per round? ({'P<partition>/<consumer>'}) ==={RESET}")
    print(f"{'Sensor':<11}{'crc32%N':>8}  " + "".join(f"{'R' + str(r):>8}" for r in rounds_done) + "   Verdict")
    stable_all = True
    shown = sensors if len(sensors) <= 40 else [t] + sensors[:15]
    for s in sensors:
        cells = [placed.get((r, s)) for r in rounds_done]
        stable = len(set(cells)) == 1 and None not in cells
        stable_all &= stable
        if s not in shown:
            continue
        text = "".join((paint(c[1], f"{f'P{c[0]}/{c[1]}':>8}") if c else f"{'?':>8}") for c in cells)
        print(f"{s:<11}{zlib.crc32(s.encode()) % args.partitions:>8}  {text}   "
              + (f"{GREEN}consistent{RESET}" if stable else f"{RED}CHANGED{RESET}"))
    if len(shown) < len(sensors):
        print(f"{DIM}... {len(sensors) - len(shown)} more sensors not printed (all are checked in the verdict below){RESET}")

    hash_ok = all(placed[(r, s)][0] == zlib.crc32(s.encode()) % args.partitions for r, s in placed)
    ack_ok = all(ack.get(k) == v[0] for k, v in placed.items())
    per_part = Counter(p for p, _ in placed.values())
    per_cons = Counter(c for _, c in placed.values())
    total = sum(per_part.values()) or 1
    sensors_per_part = Counter(placed[(rounds_done[0], s)][0] for s in sensors if (rounds_done[0], s) in placed) if rounds_done else Counter()
    print(f"\n{BOLD}=== LOAD PER PARTITION / CONSUMER ({sent_total} messages, {len(sensors)} distinct sensors) ==={RESET}")
    for p in sorted(owner):
        n = per_part[p]
        print(f"   P{p} -> {paint(owner[p], owner[p])}  {n:>5} msgs {100 * n / total:5.1f}%  "
              f"{sensors_per_part[p]:>3} sensors  {paint(owner[p], '#' * round(40 * n / total))}")
    mx, mn = max(per_part.values(), default=0), min((per_part[p] for p in owner), default=0)
    print(f"{DIM}All {args.partitions} partitions are used. Split follows crc32 of the key, so it is even in expectation "
          f"but not exactly equal (busiest/quietest = {mx / mn:.2f}).{RESET}" if mn else f"{RED}A partition received nothing.{RESET}")

    print(f"\n{BOLD}=== VERDICT ==={RESET}")
    checks = [
        ("every sensor landed on the same partition+consumer in all rounds", stable_all),
        ("partition == crc32(sensor_id) % partitions for all messages", hash_ok),
        ("consumer saw the same partition the broker ACKed", ack_ok),
        ("each partition had exactly one consumer", all(len(set(c for (_, c) in [v for v in placed.values() if v[0] == p])) == 1 for p in owner)),
        ("all partitions received data", all(per_part[p] for p in owner)),
    ]
    for text, ok in checks:
        print(f"   {GREEN + 'PASS' + RESET if ok else RED + 'FAIL' + RESET}  {text}")
    print(f"\n{DIM}Caveat: sensor -> partition is fixed while the partition count is unchanged. "
          f"partition -> consumer is fixed only while the group is stable; after a rebalance "
          f"(consumer joins/leaves) the partition moves, still to exactly one consumer. See monitoring.demo_rebalance.{RESET}")

    (out / "consistency.json").write_text(json.dumps({
        "topic": topic, "partition_owner": {str(p): c for p, c in owner.items()}, "rounds": len(rounds_done),
        "placement": {s: [list(placed[(r, s)]) if (r, s) in placed else None for r in rounds_done] for s in sensors},
        "per_partition": dict(per_part), "per_consumer": dict(per_cons),
        "all_consistent": stable_all and hash_ok and ack_ok}, indent=2), encoding="utf-8")
    print(f"Saved: output/{args.run_id}/consistency.json")
    try:
        admin.delete_topics([topic])
    except Exception:
        pass
    return 0 if all(ok for _, ok in checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
