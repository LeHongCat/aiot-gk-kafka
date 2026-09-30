"""Proof for the demo: is data split fairly across consumers, with no loss and no duplicates?

Reads offsets_*.json written by every consumer and compares them with the real
log-end offset of each partition on the broker:
  expected  = messages stored in the partition (high watermark - low watermark)
  unique    = distinct offsets read by ANY consumer of the group
  missing   = expected - unique            (must be 0 -> no loss)
  duplicates= total reads - unique         (must be 0 -> no duplicates; >0 only after crash/rebalance replay)
"""
import argparse
import json
import sys
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition
from config.settings import KAFKA_COMMON, PARTITIONS, ROOT

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def union_size(runs):
    """Number of distinct offsets in a list of [first, last] runs."""
    total, end = 0, -1
    for first, last in sorted(runs):
        first = max(first, end + 1)
        if last >= first:
            total += last - first + 1
            end = last
    return total


def broker_offsets(topic, group, partitions):
    client = Consumer({**KAFKA_COMMON, "group.id": group, "enable.auto.commit": False})
    try:
        return {p: client.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False) for p in range(partitions)}
    finally:
        client.close()


def verify(run_dir, topic=None, group=None, plot=True):
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8")) if (run_dir / "manifest.json").exists() else {}
    topic = topic or manifest.get("topic")
    group = group or manifest.get("group") or "verify"
    partitions = manifest.get("partitions", PARTITIONS)
    if not topic:
        raise ValueError("topic unknown: no manifest.json, pass --topic")
    per_consumer = defaultdict(lambda: defaultdict(int))
    runs = defaultdict(list)
    reads, replayed = defaultdict(int), defaultdict(int)
    files = list(run_dir.glob("offsets_*.json"))
    if not files:
        raise ValueError(f"{run_dir.name}: no offsets_*.json (run with the updated consumer)")
    for path in files:
        data = json.loads(path.read_text(encoding="utf-8"))
        for p, info in data["partitions"].items():
            p = int(p)
            per_consumer[data["consumer"]][p] += info["reads"]
            runs[p] += info["runs"]
            reads[p] += info["reads"]
            replayed[p] += info["replayed"]
    marks = broker_offsets(topic, group, partitions)

    print(f"\n=== Verification of {run_dir.name} (topic {topic}) ===")
    print("\n1) Per partition: did the group read every message exactly once?")
    print(f"{'Partition':>9} {'Stored':>9} {'Unique read':>12} {'Missing':>8} {'Duplicates':>11}")
    ok = True
    summary = {}
    for p in range(partitions):
        low, high = marks[p]
        expected = high - low
        unique = union_size(runs[p])
        missing, dup = expected - unique, reads[p] - unique
        ok &= missing == 0 and dup == 0
        summary[p] = {"stored": expected, "unique_read": unique, "missing": missing, "duplicates": dup}
        print(f"{p:>9} {expected:>9} {unique:>12} {missing:>8} {dup:>11}")

    total_reads = sum(sum(v.values()) for v in per_consumer.values())
    print("\n2) Per consumer: is the load shared fairly?")
    print(f"{'Consumer':>9} {'Messages':>9} {'Share':>7}  Partitions")
    shares = {}
    for cid in sorted(per_consumer):
        n = sum(per_consumer[cid].values())
        shares[cid] = n
        parts = ", ".join(f"P{p}={c}" for p, c in sorted(per_consumer[cid].items()))
        print(f"{cid:>9} {n:>9} {100 * n / total_reads:>6.1f}%  {parts}")
    ratio = max(shares.values()) / min(shares.values()) if shares and min(shares.values()) else None
    stored = [s["stored"] for s in summary.values()]
    print(f"Busiest / quietest consumer = {ratio:.2f}" if ratio else "A consumer processed nothing.")
    print(f"Busiest / quietest partition = {max(stored) / min(stored):.2f}" if min(stored) else "An empty partition exists.")
    print("Note: keys are hashed, so partitions (and consumers) are only approximately equal with few sensors.")

    verdict = "PASS: no message lost, none read twice." if ok else (
        "CHECK: missing or duplicate offsets. Duplicates are expected only after a consumer crash/rebalance "
        "(uncommitted messages are re-read = at-least-once). Missing > 0 means the run ended before the backlog was drained.")
    print(f"\n{verdict}")
    result = {"run_id": run_dir.name, "per_partition": summary, "per_consumer_messages": shares,
              "busiest_quietest_consumer_ratio": ratio, "no_loss_no_duplicates": ok}
    (run_dir / "verify.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    if plot:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots(figsize=(7, 4))
            ids = sorted(per_consumer)
            bottom = [0] * len(ids)
            for p in range(partitions):
                vals = [per_consumer[c].get(p, 0) for c in ids]
                ax.bar(ids, vals, bottom=bottom, label=f"Partition {p}")
                bottom = [b + v for b, v in zip(bottom, vals)]
            ax.set(ylabel="Messages processed", title=f"{run_dir.name}: load per consumer")
            ax.legend()
            fig.tight_layout()
            fig.savefig(run_dir / "distribution.png", dpi=150)
            plt.close(fig)
            print(f"Saved: output/{run_dir.name}/verify.json and distribution.png")
        except Exception as exc:
            print(f"(chart skipped: {exc})")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--topic")
    parser.add_argument("--group")
    args = parser.parse_args()
    run_dir = (ROOT / "output" / args.run_id).resolve()
    if run_dir.parent != (ROOT / "output").resolve() or not run_dir.exists():
        parser.error("unknown run-id")
    return 0 if verify(run_dir, args.topic, args.group)["no_loss_no_duplicates"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
