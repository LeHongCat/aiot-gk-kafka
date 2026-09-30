"""Summarize observed data only; no example numbers are shipped as results."""
import argparse
import csv
import json
import math
from collections import Counter
from pathlib import Path

from config.settings import ROOT


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def summarize(run_dir, warmup=5, extra=None):
    stats = json.loads((run_dir / "producer_stats.json").read_text(encoding="utf-8"))
    start = math.ceil(stats["start_epoch"] + warmup)
    end = math.floor(stats["send_end_epoch"])
    if end <= start:
        raise ValueError(f"{run_dir.name}: duration too short for warmup={warmup}")
    producers = Counter()
    for row in read_csv(run_dir / "producer_metrics.csv"):
        producers[int(row["epoch_second"])] += int(row["delivered"])
    consumers = Counter()
    processed = invalid = 0
    consumer_ids = set()
    for path in run_dir.glob("consumer_metrics_*.csv"):
        for row in read_csv(path):
            count = int(row["processed"])
            consumers[int(row["epoch_second"])] += count
            processed += count
            invalid += int(row["invalid"])
            consumer_ids.add(row["consumer_id"])
    if not consumer_ids:
        raise ValueError(f"{run_dir.name}: no consumer metrics")
    lags = {}
    lag_path = run_dir / "lag.csv"
    if lag_path.exists():
        for row in read_csv(lag_path):
            if row["group_lag"]:
                lags[float(row["epoch"])] = int(row["group_lag"])
    observed = [v for t, v in lags.items() if start <= t < end]
    status_path = run_dir / "run_status.json"
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    duration = end - start
    result = {
        "run_id": run_dir.name, "target_rate": stats["target_rate"],
        "consumer_count": len(consumer_ids), "warmup_s": warmup, "measurement_s": duration,
        "producer_rate": sum(producers[t] for t in range(start, end)) / duration,
        "consumer_rate": sum(consumers[t] for t in range(start, end)) / duration,
        "delivery_rate_including_flush": stats["delivery_rate_including_flush"],
        "delivered": stats["delivered"], "processed_attempts": processed,
        "invalid": invalid, "delivery_errors": stats["delivery_errors"],
        "undelivered": stats["undelivered"], "not_enqueued": stats["not_enqueued"],
        "max_lag": max(observed) if observed else None,
        "final_observed_lag": lags[max(lags)] if lags else None,
        "drained": status.get("drained"), "drain_s": status.get("drain_s"),
        "consumer_delay_ms": status.get("consumer_delay_ms"),
    }
    result.update(extra or {})
    with (run_dir / "benchmark_results.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(result))
        writer.writeheader()
        writer.writerow(result)
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    xs = list(range(start, end))
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot([x - start for x in xs], [producers[x] for x in xs], label="Producer: broker ACK")
    ax.plot([x - start for x in xs], [consumers[x] for x in xs], label="Consumers: valid processing attempts")
    ax.axhline(stats["target_rate"], linestyle="--", color="grey", label="Target")
    ax.set(xlabel="Seconds after measurement start", ylabel="Messages/second", title=run_dir.name)
    ax.legend()
    fig.tight_layout()
    fig.savefig(run_dir / "throughput.png", dpi=150)
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(10, 4))
    ts = sorted(lags)
    ax.plot([t - stats["start_epoch"] for t in ts], [lags[t] for t in ts])
    ax.axvline(stats["send_end_epoch"] - stats["start_epoch"], linestyle="--", color="grey", label="Producer send ended")
    if stats.get("spike_rate"):
        ax.axvspan(stats["spike_start_s"], stats["spike_start_s"] + stats["spike_duration_s"], alpha=0.2, color="orange",
                   label=f"Spike {stats['spike_rate']} msg/s")
    ax.set(xlabel="Seconds after producer start", ylabel="Group committed lag", title=run_dir.name)
    ax.legend()
    fig.tight_layout()
    fig.savefig(run_dir / "consumer_lag.png", dpi=150)
    plt.close(fig)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--warmup", type=float, default=5)
    args = parser.parse_args()
    if args.warmup < 0:
        parser.error("warmup must be nonnegative")
    run_dir = (ROOT / "output" / args.run_id).resolve()
    if run_dir.parent != (ROOT / "output").resolve():
        parser.error("invalid run-id")
    print(json.dumps(summarize(run_dir, args.warmup), indent=2))


if __name__ == "__main__":
    main()
