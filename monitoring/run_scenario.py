"""One automated benchmark run: new topic + group + output dir, N consumers, producer,
lag monitor, wait for drain, then summarize. Prints step-by-step progress."""
import argparse
import json
import subprocess
import sys
import time
from collections import defaultdict

from confluent_kafka.admin import AdminClient, NewTopic
from config.settings import KAFKA_COMMON, PARTITIONS, ROOT, output_dir
from monitoring.benchmark import summarize
from monitoring.lag_monitor import observer, read_lag
from monitoring.resources import ResourceSampler
from monitoring.verify import verify

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def step(n, total, text):
    print(f"\n[{n}/{total}] {text}", flush=True)


def bar(value, scale, width=30):
    filled = min(width, int(width * value / scale)) if scale else 0
    return "#" * filled + "-" * (width - filled)


def group_lag(client, topic):
    rows = read_lag(client, topic, PARTITIONS)
    return sum(r["lag"] for r in rows) if all(r["lag"] is not None for r in rows) else None


def spawn(module, args, log_path):
    log = log_path.open("w", encoding="utf-8")
    return subprocess.Popen([sys.executable, "-m", module, *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT), log


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rate", type=int, required=True, help="target messages/second")
    parser.add_argument("--consumers", type=int, required=True)
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--warmup", type=float, default=5)
    parser.add_argument("--delay-ms", type=float, default=0, help="simulated slow consumer (per message)")
    parser.add_argument("--drain-timeout", type=float, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--spike-rate", type=int, default=0, help="burst rate (msg/s) to test a sudden load increase")
    parser.add_argument("--spike-start", type=float, default=0)
    parser.add_argument("--spike-duration", type=float, default=0)
    args = parser.parse_args()
    if args.rate <= 0 or args.consumers <= 0 or args.duration <= 0:
        parser.error("rate, consumers, duration must be positive")
    spike_args = (["--spike-rate", str(args.spike_rate), "--spike-start", str(args.spike_start),
                   "--spike-duration", str(args.spike_duration)] if args.spike_rate else [])

    out = output_dir(args.run_id)
    if (out / "producer.started").exists():
        raise SystemExit(f"output/{args.run_id} already has data. Use a new --run-id.")
    topic, group = f"bench-{args.run_id}", f"grp-{args.run_id}"
    (out / "manifest.json").write_text(json.dumps({
        "run_id": args.run_id, "topic": topic, "group": group, "partitions": PARTITIONS,
        "rate": args.rate, "consumers": args.consumers, "duration": args.duration, "warmup": args.warmup,
        "delay_ms": args.delay_ms, "seed": args.seed, "spike_rate": args.spike_rate, "spike_start": args.spike_start,
        "spike_duration": args.spike_duration, "python": sys.version, "started": time.time()}, indent=2), encoding="utf-8")

    print(f"=== BENCHMARK {args.run_id}: {args.rate} msg/s x {args.duration:.0f}s, {args.consumers} consumer, "
          f"{PARTITIONS} partition, delay={args.delay_ms} ms ===")
    total_steps = 6
    step(1, total_steps, f"Creating topic '{topic}' ({PARTITIONS} partitions)")
    admin = AdminClient({**KAFKA_COMMON})  # keep a reference: if it is garbage-collected, the futures fail with _DESTROY
    for fut in admin.create_topics([NewTopic(topic, PARTITIONS, 1)]).values():
        fut.result(20)

    procs = []
    step(2, total_steps, f"Starting {args.consumers} consumer(s) and waiting for Kafka to assign partitions")
    for i in range(args.consumers):
        cid = f"C{i + 1}"
        procs.append((cid, *spawn("consumer.consumer", ["--id", cid, "--run-id", args.run_id, "--topic", topic,
                                                       "--group", group, "--delay-ms", str(args.delay_ms)], out / f"{cid}.log")))
    deadline = time.time() + 60
    while time.time() < deadline and len(list(out.glob("ready_C*.json"))) < args.consumers:
        time.sleep(0.5)
    if len(list(out.glob("ready_C*.json"))) < args.consumers:
        raise SystemExit(f"Consumers not ready after 60s, see the logs in output/{args.run_id}/C*.log")
    time.sleep(3)  # let rebalancing settle before load starts
    monitor, monitor_log = spawn("monitoring.lag_monitor", ["--topic", topic, "--group", group, "--run-id", args.run_id], out / "lag_monitor.log")
    client = observer(group)
    running = [p for _, p, _ in procs] + [monitor]
    sampler = ResourceSampler(out / "resources.csv", lambda: running)
    sampler.start()

    step(3, total_steps, f"Sending load: producer pushes {args.rate} msg/s for {args.duration:.0f}s")
    print(f"{'sec':>5} | {'group lag':>13} | lag bar (full bar = 1 second of load)")
    producer, producer_log = spawn("producer.producer", ["--rate", str(args.rate), "--duration", str(args.duration),
                                                          "--topic", topic, "--run-id", args.run_id, "--seed", str(args.seed), *spike_args], out / "producer.log")
    running.append(producer)
    t0 = time.time()
    while producer.poll() is None:
        time.sleep(1)
        try:
            lag = group_lag(client, topic)
        except Exception:
            lag = None
        shown = "?" if lag is None else str(lag)
        print(f"{time.time() - t0:5.0f} | {shown:>13} | {bar(lag or 0, args.rate)}", flush=True)
    producer_log.close()

    step(4, total_steps, f"Producer finished (exit={producer.returncode}). Waiting for consumers to drain the backlog (max {args.drain_timeout:.0f}s)")
    drain_start = time.time()
    drained = False
    while time.time() - drain_start < args.drain_timeout:
        try:
            lag = group_lag(client, topic)
        except Exception:
            lag = None
        print(f"{time.time() - drain_start:5.0f}s | remaining {('?' if lag is None else lag):>8} messages", flush=True)
        if lag == 0:
            drained = True
            break
        time.sleep(1)
    drain_s = time.time() - drain_start
    print("  -> " + ("backlog drained" if drained else "TIMEOUT, backlog not drained (recorded as drained=false)"))

    step(5, total_steps, "Stopping consumers and lag monitor")
    sampler.stop()
    (out / "stop.requested").write_text("1", encoding="utf-8")
    exit_codes = {}
    for name, proc, log in [*procs, ("lag_monitor", monitor, monitor_log)]:
        try:
            proc.wait(20)
        except subprocess.TimeoutExpired:
            proc.kill()
        exit_codes[name] = proc.returncode
        log.close()
    client.close()
    (out / "run_status.json").write_text(json.dumps({
        "producer_exit": producer.returncode, "exit_codes": exit_codes, "drained": drained,
        "drain_s": drain_s, "consumer_delay_ms": args.delay_ms}, indent=2), encoding="utf-8")

    step(6, total_steps, "Summarizing results")
    res = sampler.summary()
    if sampler.error:
        print(f"  (resource sampling warning: {sampler.error})")
    try:
        sampler.plot(out / "resources.png", args.run_id)
    except Exception as exc:
        print(f"  (resources.png skipped: {exc})")
    r = summarize(out, args.warmup, extra=res)
    fmt = lambda v, s="": "n/a" if v is None else f"{v:,.0f}{s}"
    print("\n+--------------------------------------------------+")
    print(f"| Target rate            : {args.rate:>10,} msg/s              |")
    print(f"| Producer actual (ACK)  : {r['producer_rate']:>10,.0f} msg/s              |")
    print(f"| Consumer actual        : {r['consumer_rate']:>10,.0f} msg/s              |")
    print(f"| Max lag                : {fmt(r['max_lag']):>10} messages           |")
    print(f"| Drain time             : {drain_s:>10.1f} s   drained={drained!s:<5}   |")
    print(f"| Send errors / invalid  : {r['delivery_errors']:>4} / {r['invalid']:<4}                     |")
    print("+--------------------------------------------------+")
    print(f"Kafka container : CPU avg {fmt(res['kafka_cpu_avg_pct'])}% / max {fmt(res['kafka_cpu_max_pct'])}% (100% = 1 core), "
          f"RAM max {fmt(res['kafka_mem_max_mb'])} MB")
    print(f"Python processes: CPU avg {fmt(res['python_cpu_avg_pct'])}% / max {fmt(res['python_cpu_max_pct'])}%, "
          f"RAM max {fmt(res['python_rss_max_mb'])} MB (producer + consumers + lag monitor)")

    try:
        verify(out, topic, group)
    except Exception as exc:
        print(f"(verification skipped: {exc})")

    owners = defaultdict(set)
    for path in out.glob("sensor_map_*.json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        for sensor in data["sensors"]:
            owners[sensor].add(data["consumer"])
    split = sorted(s for s, o in owners.items() if len(o) > 1)
    print(f"1:1 check: {len(owners) - len(split)}/{len(owners)} sensors were processed by exactly 1 consumer"
          + (f"; split sensors (rebalance/replay): {', '.join(split[:5])}" if split else ""))
    print(f"\nResults: output/{args.run_id}/  (benchmark_results.csv, throughput.png, consumer_lag.png, resources.png, distribution.png, verify.json)")
    bad = producer.returncode or any(exit_codes.values())
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
