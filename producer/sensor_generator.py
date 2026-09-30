"""Synthetic load data, NOT a validated motor/physics model."""
import json
import random
from datetime import datetime, timezone


def generate_sensor(rng, num_sensors=100):
    return {
        "sensor_id": f"motor_{rng.randint(1, num_sensors):03d}",
        "temperature": round(rng.uniform(40, 100), 2),
        "humidity": round(rng.uniform(30, 80), 2),
        "voltage": round(rng.uniform(210, 230), 2),
        "current": round(rng.uniform(1, 10), 3),
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


if __name__ == "__main__":
    rng = random.Random(42)
    for _ in range(5):
        print(json.dumps(generate_sensor(rng)))
