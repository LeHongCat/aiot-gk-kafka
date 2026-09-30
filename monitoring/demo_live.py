"""Live walkthrough: follow each message through  generate -> send -> Kafka stores it -> a consumer processes it.

A message is identified by the (partition, offset) Kafka assigns to it: the producer learns it from the
broker's ACK, the consumer reads it from the record, so both sides can be matched without changing the schema.

  python -m monitoring.demo_live                 # 3 consumers, keyed by sensor_id
  python -m monitoring.demo_live --step          # press Enter between messages (best for presenting)
  python -m monitoring.demo_live --pretty        # multi-line JSON
  python -m monitoring.demo_live --no-key        # contrast: no key, sensors get spread over consumers
"""
import argparse
import json
import os
import random
import sys
import threading
import time
import zlib
from collections import Counter, defaultdict

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_COMMON, output_dir
from consumer.validation import validate_event
from producer.sensor_generator import generate_sensor

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

COLORS = ["\033[36m", "\033[33m", "\033[35m", "\033[32m", "\033[34m", "\033[31m"]
RESET, BOLD, DIM, RED, GREEN = "\033[0m", "\033[1m", "\033[2m", "\033[31m", "\033[32m"
OVERHEAT_C = 90  # demo rule only: the consumer flags temperature above this


def paint(idx, text):
    return f"{COLORS[idx % len(COLORS)]}{text}{RESET}"


def cidx(cid):
    return int(cid[1:]) - 1


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default="live01")
    parser.add_argument("--sensors", type=int, default=5)
    parser.add_argument("--messages", type=int, default=15)
    parser.add_argument("--consumers", type=int, default=3)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--interval", type=float, default=0.8, help="seconds between messages (ignored with --step)")
    parser.add_argument("--step", action="store_true", help="wait for Enter before each message")
    parser.add_argument("--pretty", action="store_true", help="print the JSON on several lines")
    parser.add_argument("--panel-every", type=int, default=5, help="show the partition logs every N messages (0 = only at the end)")
    parser.add_argument("--no-key", action="store_true", help="send without a key")
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()
    if min(args.sensors, args.messages, args.consumers, args.partitions) < 1 or args.interval < 0:
        parser.error("counts must be >= 1 and --interval >= 0")

    os.system("")  # enable ANSI colors in Windows terminals
    out = output_dir(args.run_id)
    stamp = int(time.time())
    topic = group = f"live-{args.run_id}-{stamp}"
    admin = AdminClient({**KAFKA_COMMON})  # keep the reference until the futures complete
    for fut in admin.create_topics([NewTopic(topic, args.partitions, 1)]).values():
        fut.result(15)

    arrivals = {}   # (partition, offset) -> (consumer, receive time, raw value)
    assigned = {}
    lock = threading.Lock()
    stop = threading.Event()

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
                    arrivals[(msg.partition(), msg.offset())] = (cid, time.time(), msg.value())
        c.close()

    threads = [threading.Thread(target=consumer_thread, args=(i,), daemon=True) for i in range(args.consumers)]
    for t in threads:
        t.start()

    print(f"\n{BOLD}=== LIVE DEMO: the journey of each message ==={RESET}")
    print(f"Topic {topic} | {args.partitions} partitions | {args.consumers} consumers in one group | "
          f"{'NO key' if args.no_key else 'key = sensor_id'}")
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

    print(f"\n{BOLD}Architecture{RESET}")
    print("  Producer --> [ Topic ] --> Consumer Group (each partition has exactly ONE consumer)")
    for p in range(args.partitions):
        cid = owner.get(p)
        print(f"               Partition {p}  ==>  " + (paint(cidx(cid), f"Consumer {cid}") if cid else "(nobody)"))
    print(f"\n{DIM}Legend: each message is followed through 4 stages. A message is identified by "
          f"(partition, offset).{RESET}")

    producer = Producer({**KAFKA_COMMON, "linger.ms": 0})
    rng = random.Random(args.seed)
    partition_log = defaultdict(list)
    trace = []
    sensor_seen = defaultdict(lambda: {"partitions": set(), "consumers": set(), "offsets": []})
    per_consumer = Counter()
    crc_ok = crc_total = 0

    def panel(title):
        print(f"\n{BOLD}--- {title} ---{RESET}")
        for p in range(args.partitions):
            entries = partition_log[p][-6:]
            cid = owner.get(p, "?")
            cells = " | ".join(f"{paint(cidx(c), f'{o}:{s[-3:]}')}" for o, s, c in entries) or "(empty)"
            more = f"{DIM}...{RESET} " if len(partition_log[p]) > 6 else ""
            print(f"  P{p} (owner {paint(cidx(cid), cid) if cid != '?' else '?'}): {more}[ {cells} ]   {DIM}offset:sensor{RESET}")

    sent = 0
    try:
        for seq in range(1, args.messages + 1):
            if args.step:
                try:
                    input(f"\n{DIM}[Enter] send message {seq}/{args.messages}  (Ctrl+C to stop){RESET}")
                except EOFError:
                    args.step = False
            event = generate_sensor(rng, args.sensors)
            t_gen = time.time()
            payload = json.dumps(event, separators=(",", ":")).encode()
            key = None if args.no_key else event["sensor_id"].encode()

            print(f"\n{BOLD}#{seq:02d}{RESET} " + "-" * 70)
            print(f"{BOLD}1. GENERATE{RESET}  producer creates a sensor reading")
            if args.pretty:
                print("   " + json.dumps(event, indent=2).replace("\n", "\n   "))
            else:
                print(f"   {json.dumps(event, separators=(', ', ': '))}")

            print(f"{BOLD}2. SEND{RESET}      key={key.decode() if key else 'none'}  value={len(payload)} bytes  -> topic {topic}")
            if key:
                predicted = zlib.crc32(key) % args.partitions
                print(f"   {DIM}partitioner: crc32('{key.decode()}') % {args.partitions} = {predicted}{RESET}")
            result = {}

            def on_delivery(err, msg, result=result):
                result.update(err=err, t=time.time(), partition=None if err else msg.partition(),
                              offset=None if err else msg.offset())

            t_send = time.time()
            producer.produce(topic, key=key, value=payload, on_delivery=on_delivery)
            producer.flush(10)
            if result.get("err"):
                print(f"   {RED}send failed: {result['err']}{RESET}")
                continue
            p, off = result["partition"], result["offset"]
            sent += 1
            note = ""
            if key:
                crc_total += 1
                if p == predicted:
                    crc_ok += 1
                    note = f"  {GREEN}(matches the crc32 prediction){RESET}"
                else:
                    note = f"  {DIM}(differs from the crc32 formula above){RESET}"
            print(f"{BOLD}3. KAFKA{RESET}     broker stored it: partition={BOLD}{p}{RESET} offset={BOLD}{off}{RESET}, "
                  f"acknowledged in {(result['t'] - t_send) * 1000:.1f} ms{note}")

            end = time.time() + 5
            while (p, off) not in arrivals and time.time() < end:
                time.sleep(0.005)
            got = arrivals.get((p, off))
            if not got:
                print(f"{BOLD}4. CONSUME{RESET}   {RED}no consumer received it within 5 s{RESET}")
                continue
            cid, t_recv, raw = got
            rec = json.loads(raw)
            try:
                validate_event(rec)
                verdict = f"{GREEN}valid{RESET}"
                alert = f"  {RED}ALERT overheating ({rec['temperature']} C > {OVERHEAT_C}, demo rule){RESET}" \
                    if rec["temperature"] > OVERHEAT_C else f"  temperature {rec['temperature']} C normal"
            except ValueError as exc:
                verdict, alert = f"{RED}invalid: {exc}{RESET}", ""
            print(f"{BOLD}4. CONSUME{RESET}   {paint(cidx(cid), 'Consumer ' + cid)} read P{p}@{off}, schema {verdict},"
                  f"{alert}")
            print(f"   {DIM}end-to-end latency (generate -> consumed): {(t_recv - t_gen) * 1000:.1f} ms{RESET}")

            sensor = rec["sensor_id"]
            partition_log[p].append((off, sensor, cid))
            per_consumer[cid] += 1
            info = sensor_seen[sensor]
            info["partitions"].add(p)
            info["consumers"].add(cid)
            info["offsets"].append((p, off))
            trace.append({"seq": seq, "sensor_id": sensor, "key": key.decode() if key else None, "partition": p,
                          "offset": off, "consumer": cid, "latency_ms": round((t_recv - t_gen) * 1000, 2),
                          "ack_ms": round((result["t"] - t_send) * 1000, 2), "event": event})
            if args.panel_every and seq % args.panel_every == 0 and seq != args.messages:
                panel(f"What Kafka stores after {seq} messages")
            if not args.step:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopped by user.")

    stop.set()
    for t in threads:
        t.join(5)
    panel(f"Final content of each partition (last 6 records)")
    print(f"\n{BOLD}=== SUMMARY ==={RESET}")
    print(f"Sent {sent}, consumed {sum(per_consumer.values())}")
    for cid in sorted(per_consumer):
        print(f"  {paint(cidx(cid), cid)} processed {per_consumer[cid]} message(s) from partitions "
              f"{sorted(p for p, c in owner.items() if c == cid)}")
    print(f"\n{'Sensor':<11}{'Partition(s)':<14}{'Consumer(s)':<14}Order within sensor")
    all_one = True
    for sensor in sorted(sensor_seen):
        info = sensor_seen[sensor]
        offs = [o for _, o in info["offsets"]]
        ordered = len(info["partitions"]) == 1 and offs == sorted(offs)
        one = len(info["consumers"]) == 1
        all_one &= one
        print(f"{sensor:<11}{','.join(map(str, sorted(info['partitions']))):<14}{','.join(sorted(info['consumers'])):<14}"
              f"{GREEN + 'ordered (offsets increase)' + RESET if ordered else RED + 'NOT ordered: spread over partitions' + RESET}"
              f"   {GREEN + '1:1' + RESET if one else RED + 'split' + RESET}")
    if crc_total:
        print(f"\nThe crc32 formula predicted the partition in {crc_ok}/{crc_total} messages.")
    print(f"\n{BOLD}{GREEN + 'Every sensor was handled by exactly one consumer and kept its order.' + RESET if all_one else RED + 'Some sensors were split across consumers (no key -> no per-sensor routing or ordering).' + RESET}{RESET}")

    trace_path = out / "live_trace.jsonl"
    trace_path.write_text("".join(json.dumps(row) + "\n" for row in trace), encoding="utf-8")
    print(f"\nFull trace saved: output/{args.run_id}/live_trace.jsonl")
    try:
        admin.delete_topics([topic])
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
