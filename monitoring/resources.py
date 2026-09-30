"""Samples CPU/RAM once per second during a benchmark run.

kafka_*  : the Kafka container, from `docker stats` (CPU % is relative to ONE core, so 200% = two cores busy).
python_* : producer + consumers + lag monitor processes, from psutil (same CPU convention).
"""
import csv
import re
import subprocess
import threading
import time

import psutil

from config.settings import ROOT

UNITS = {"b": 1 / 1024 ** 2, "kib": 1 / 1024, "kb": 1 / 1000, "mib": 1.0, "mb": 1.0, "gib": 1024.0, "gb": 1000.0}


def _mb(text):
    m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
    return float(m.group(1)) * UNITS.get(m.group(2).lower(), 1.0) if m else None


def kafka_container():
    out = subprocess.run(["docker", "compose", "ps", "-q", "kafka"], cwd=ROOT, capture_output=True, text=True, timeout=30)
    return out.stdout.strip().splitlines()[0] if out.stdout.strip() else None


def docker_sample(container):
    out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}|{{.MemUsage}}", container],
                         capture_output=True, text=True, timeout=30)
    cpu, mem = out.stdout.strip().split("|", 1)
    return float(cpu.strip("% ")), _mb(mem.split("/")[0])


class ResourceSampler:
    def __init__(self, path, processes):
        """`processes`: callable returning the current list of subprocess.Popen objects to measure."""
        self.path, self.processes = path, processes
        self.rows = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._cache = {}
        self.container = None
        self.error = None

    def start(self):
        try:
            self.container = kafka_container()
        except Exception as exc:
            self.error = f"docker lookup failed: {exc}"
        self._thread.start()

    def _python_usage(self):
        """Sum over each launched process AND its children: on Windows a venv's python.exe is a small
        launcher that starts the real interpreter as a child, so measuring only the launcher shows ~0%."""
        cpu = rss = 0.0
        seen = set()
        for proc in self.processes():
            try:
                root = self._cache.setdefault(proc.pid, psutil.Process(proc.pid))
                tree = [root, *root.children(recursive=True)]
            except (psutil.Error, OSError):
                continue
            for ps in tree:
                if ps.pid in seen:
                    continue
                seen.add(ps.pid)
                ps = self._cache.setdefault(ps.pid, ps)   # reuse the object so cpu_percent has a previous sample
                try:
                    cpu += ps.cpu_percent(interval=None)
                    rss += ps.memory_info().rss / 1024 ** 2
                except (psutil.Error, OSError):
                    continue
        return cpu, rss

    def _run(self):
        while not self._stop.is_set():
            began = time.time()
            k_cpu = k_mem = None
            if self.container:
                try:
                    k_cpu, k_mem = docker_sample(self.container)
                except Exception as exc:
                    self.error = f"docker stats failed: {exc}"
            p_cpu, p_mem = self._python_usage()
            self.rows.append([time.time(), k_cpu, k_mem, p_cpu, p_mem])
            self._stop.wait(max(0.0, 1.0 - (time.time() - began)))

    def stop(self):
        self._stop.set()
        self._thread.join(10)
        with open(self.path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["epoch", "kafka_cpu_pct", "kafka_mem_mb", "python_cpu_pct", "python_rss_mb"])
            writer.writerows(self.rows)

    def summary(self):
        def col(i, fn):
            vals = [r[i] for r in self.rows if r[i] is not None]
            return round(fn(vals), 1) if vals else None
        avg = lambda v: sum(v) / len(v)
        return {"kafka_cpu_avg_pct": col(1, avg), "kafka_cpu_max_pct": col(1, max), "kafka_mem_max_mb": col(2, max),
                "python_cpu_avg_pct": col(3, avg), "python_cpu_max_pct": col(3, max), "python_rss_max_mb": col(4, max)}

    def plot(self, png_path, title):
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        if not self.rows:
            return
        t0 = self.rows[0][0]
        xs = [r[0] - t0 for r in self.rows]
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
        ax1.plot(xs, [r[1] for r in self.rows], label="Kafka container")
        ax1.plot(xs, [r[3] for r in self.rows], label="Python processes")
        ax1.set(ylabel="CPU % (100 = one core)", title=title)
        ax1.legend()
        ax2.plot(xs, [r[2] for r in self.rows], label="Kafka container")
        ax2.plot(xs, [r[4] for r in self.rows], label="Python processes")
        ax2.set(xlabel="Seconds since run start", ylabel="RAM (MB)")
        ax2.legend()
        fig.tight_layout()
        fig.savefig(png_path, dpi=150)
        plt.close(fig)
