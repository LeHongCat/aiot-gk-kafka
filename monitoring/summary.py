"""Merge benchmark_results.csv of all T*-r* runs into one report table (mean, min-max)."""
import csv
import re
from collections import defaultdict
from statistics import mean

from config.settings import ROOT

FIELDS = [("producer_rate", "Producer (msg/s)"), ("consumer_rate", "Consumer (msg/s)"),
          ("max_lag", "Max lag"), ("drain_s", "Drain (s)"), ("delivery_errors", "Send errors"),
          ("kafka_cpu_avg_pct", "Kafka CPU avg %"), ("kafka_mem_max_mb", "Kafka RAM max MB"),
          ("python_cpu_avg_pct", "Python CPU avg %"), ("python_rss_max_mb", "Python RAM max MB")]


def main():
    groups = defaultdict(list)
    for path in sorted((ROOT / "output").glob("*/benchmark_results.csv")):
        m = re.fullmatch(r"([A-Z]+\d+)-r\d+", path.parent.name)
        if m:
            with path.open(newline="", encoding="utf-8") as f:
                groups[m.group(1)].append(next(csv.DictReader(f)))
    if not groups:
        print("No T*-r* runs found in output/. Run the T1-T4 benchmarks first.")
        return
    out_rows = []
    print(f"{'Test':6}{'Target':>8}{'Cons':>5}{'Runs':>4}" + "".join(f"{label:>26}" for _, label in FIELDS) + "  Drained")
    for name in sorted(groups):
        runs = groups[name]
        cells, row = [], {"test": name, "target_rate": runs[0]["target_rate"], "consumers": runs[0]["consumer_count"], "runs": len(runs)}
        for key, _ in FIELDS:
            vals = [float(r[key]) for r in runs if r.get(key) not in (None, "", "None")]
            if vals:
                cells.append(f"{mean(vals):,.0f} ({min(vals):,.0f}-{max(vals):,.0f})")
                row[key] = round(mean(vals), 1)
            else:
                cells.append("n/a")
        drained = f"{sum(r['drained'] == 'True' for r in runs)}/{len(runs)}"
        print(f"{name:6}{runs[0]['target_rate']:>8}{runs[0]['consumer_count']:>5}{len(runs):>4}" + "".join(f"{c:>26}" for c in cells) + f"  {drained}")
        row["drained"] = drained
        out_rows.append(row)
    target = ROOT / "output" / "summary.csv"
    with target.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sorted({k for r in out_rows for k in r}))
        writer.writeheader()
        writer.writerows(out_rows)
    print(f"\nSaved: {target}")


if __name__ == "__main__":
    main()
