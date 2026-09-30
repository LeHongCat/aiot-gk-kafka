"""Small local-lab config loader: KEY=VALUE only; OS env has priority."""
import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
env_path = ROOT / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("\"'"))

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# Docker publishes the broker on IPv4 only (127.0.0.1), but Windows resolves "localhost" to ::1 first;
# every failed IPv6 attempt costs ~2 s. Force IPv4 for all clients.
KAFKA_COMMON = {"bootstrap.servers": BOOTSTRAP, "broker.address.family": "v4"}
TOPIC = os.getenv("KAFKA_TOPIC", "sensor-data")
GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "aiot-group")
PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "3"))
RATE = int(os.getenv("PRODUCER_RATE", "5000"))
DURATION = float(os.getenv("PRODUCER_DURATION", "60"))
NUM_SENSORS = int(os.getenv("NUM_SENSORS", "100"))


def output_dir(run_id):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", run_id):
        raise ValueError("run-id: 1–64 letters, numbers, underscore or hyphen")
    path = ROOT / "output" / run_id
    path.mkdir(parents=True, exist_ok=True)
    return path
