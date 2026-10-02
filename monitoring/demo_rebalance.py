"""Automatic demo: 3 consumers in one group -> stop C2 -> restart C2 while the producer keeps sending.

Uses its own topic/group/output per run-id and prints the partition assignment table at each phase.
"""
import argparse
import json
import signal
import subprocess
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_COMMON, TOPIC, GROUP, PARTITIONS, ROOT, output_dir

IDS = ["C1", "C2", "C3"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rate", type=int, default=1000)
    parser.add_argument("--duration", type=float, default=75)
    parser.add_argument("--hold", type=float, default=8, help="seconds to hold each phase so you can observe it")
    parser.add_argument("--crash", action="store_true", help="kill C2 instead of Ctrl+C (waits for the session timeout, ~10s)")
    args = parser.parse_args()
    if (ROOT / "output" / args.run_id).exists():
        parser.error("run-id already exists: choose a new run-id")
    out = output_dir(args.run_id)
    topic, group = f"{TOPIC}-{args.run_id}", f"{GROUP}-{args.run_id}"

    print(f"Topic: {topic}\nGroup: {group}\n(Optional web view: start kafka-ui, then open Consumers -> {group})", flush=True)
    admin = AdminClient({**KAFKA_COMMON})  # keep a reference until the future completes
    admin.create_topics([NewTopic(topic, num_partitions=PARTITIONS, replication_factor=1)])[topic].result(timeout=30)

    procs, handles = {}, []
    t0 = time.time()

    def launch(name, module, extra):
        handle = (out / f"{name}.log").open("a", encoding="utf-8")
        handles.append(handle)
        procs[name] = subprocess.Popen(
            [sys.executable, "-u", "-m", module, "--run-id", args.run_id, "--topic", topic, *extra],
            cwd=ROOT, stdin=subprocess.DEVNULL, stdout=handle, stderr=subprocess.STDOUT,
            # Windows cannot send SIGINT to a child; a separate process group lets us send CTRL_BREAK instead.
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0)

    def assignment(ids):
        table = {}
        for cid in IDS:
            try:
                table[cid] = json.loads((out / f"ready_{cid}.json").read_text())["partitions"] if cid in ids else None
            except (FileNotFoundError, json.JSONDecodeError):
                table[cid] = []
        return table

    def show(title, ids):
        print(f"\n[{time.time() - t0:5.1f}s] {title}", flush=True)
        for cid, parts in assignment(ids).items():
            print(f"    {cid}: " + ("(stopped)" if parts is None else f"partitions {sorted(parts)}" if parts else "no partitions yet"), flush=True)

    def wait_full(ids, timeout=60):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            got = sorted(p for parts in assignment(ids).values() if parts for p in parts)
            if got == list(range(PARTITIONS)):
                time.sleep(2)  # the assignment must stay unchanged over a stable interval
                if sorted(p for parts in assignment(ids).values() if parts for p in parts) == list(range(PARTITIONS)):
                    return
            if any(p.poll() is not None for n, p in procs.items() if n in ids):
                raise RuntimeError("A consumer exited unexpectedly: see output/<run-id>/C*.log")
            time.sleep(0.5)
        raise TimeoutError(f"Not all {PARTITIONS} partitions were assigned: check the logs")

    rc = None
    try:
        for cid in IDS:
            launch(cid, "consumer.consumer", ["--id", cid, "--group", group])
        wait_full(IDS)
        launch("lag_monitor", "monitoring.lag_monitor", ["--group", group])
        launch("producer", "producer.producer", ["--rate", str(args.rate), "--duration", str(args.duration)])
        events = [("producer started sending", time.time())]
        show("PHASE 1: 3 consumers stable, producer is sending", IDS)
        time.sleep(args.hold)

        events.append(("stopped C2", time.time()))
        print(f"\n>>> {'KILL' if args.crash else 'Ctrl+C'} C2", flush=True)
        if args.crash:
            procs["C2"].kill()
        else:
            procs["C2"].send_signal(signal.CTRL_BREAK_EVENT if sys.platform == "win32" else signal.SIGINT)
        procs["C2"].wait(timeout=30)
        alive = ["C1", "C3"]
        wait_full(alive)
        events.append((f"C1+C3 own all {PARTITIONS} partitions", time.time()))
        show(f"PHASE 2: C2 stopped, C1 and C3 now share all {PARTITIONS} partitions", alive)
        time.sleep(args.hold)

        events.append(("restarted C2", time.time()))
        print("\n>>> Restarting C2", flush=True)
        launch("C2", "consumer.consumer", ["--id", "C2", "--group", group])
        wait_full(IDS)
        events.append(("partitions reassigned to all 3 consumers", time.time()))
        show("PHASE 3: C2 is back, the group rebalanced the partitions", IDS)

        rc = procs["producer"].wait(timeout=args.duration + 90)
    finally:
        time.sleep(3)
        (out / "stop.requested").touch()
        for proc in procs.values():
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.terminate()
        for handle in handles:
            handle.close()

    print("\n=== Timeline of actions (seconds since the producer started) ===")
    base = events[0][1]
    for name, ts in events:
        print(f"  +{ts - base:5.1f}s  {name}")
    print("\n=== Consumer rebalance log (assign/revoke/lost) ===")
    rows = []
    for path in out.glob("rebalance_*.jsonl"):
        rows += [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    for row in sorted(rows, key=lambda r: r["epoch"]):
        print(f"  +{row['epoch'] - base:6.1f}s  {row['consumer_id']}  {row['event']:7s} partitions={row['partitions']}")
    stats = json.loads((out / "producer_stats.json").read_text(encoding="utf-8"))
    print(f"\nProducer: delivered={stats['delivered']} errors={stats['delivery_errors']} undelivered={stats['undelivered']}")
    try:
        from monitoring.benchmark import summarize
        summarize(out, 5)
        print(f"Charts: output/{args.run_id}/throughput.png, consumer_lag.png")
    except Exception as exc:
        print(f"(charts skipped: {exc})")
    return 0 if rc == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
