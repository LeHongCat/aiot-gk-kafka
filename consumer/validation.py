import math
import re
from datetime import datetime


def validate_event(event):
    if not isinstance(event, dict):
        raise ValueError("JSON value must be an object")
    if not isinstance(event.get("sensor_id"), str) or not re.fullmatch(r"motor_[0-9]+", event["sensor_id"]):
        raise ValueError("invalid sensor_id")
    for field in ("temperature", "humidity", "voltage", "current"):
        value = event.get(field)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"invalid numeric field: {field}")
    value = event.get("timestamp")
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("timestamp must be UTC ending in Z")
    datetime.fromisoformat(value[:-1] + "+00:00")
    # Normal generator ranges are not schema constraints: a high temperature
    # can be valid telemetry and later an anomaly, not malformed input.
