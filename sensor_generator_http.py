"""
Generator that sends its readings to the Kafka ingestion API over HTTP.
Keep this file next to Mostafa_work.py.

    pip install requests pandas psycopg2-binary
    python sensor_generator_http.py        # Ctrl+C to stop
"""

import os
import time

import requests

import simulator as sensors   # DEVICES + generate_reading (unchanged)

API_URL = os.environ.get("API_URL", "http://localhost:8001")
MAX_PENDING = 1000        # the API accepts at most 1000 readings per request


def send(batch):
    """Return 'ok', 'drop' (the API rejected it, retrying is useless) or 'retry'."""
    try:
        resp = requests.post(f"{API_URL}/readings/batch", json=batch, timeout=5)
    except requests.RequestException as e:
        print("API unreachable, will retry:", e)
        return "retry"
    if resp.status_code == 202:
        return "ok"
    if 400 <= resp.status_code < 500:
        print("API rejected the data:", resp.status_code, resp.text[:200])
        return "drop"
    print("API error, will retry:", resp.status_code)
    return "retry"


def run(interval=1, duration=None):
    pending = []                         # readings not yet accepted (kept if the API is down)
    start = time.time()
    print(f"Sending readings to {API_URL}. Ctrl+C to stop.")
    try:
        while duration is None or (time.time() - start) < duration:
            for device_id, info in sensors.DEVICES.items():
                pending.append(sensors.generate_reading(device_id, info))
            pending = pending[-MAX_PENDING:]

            result = send(pending)
            if result in ("ok", "drop"):
                if result == "ok":
                    print(f"Sent {len(pending)} readings")
                pending = []
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nGenerator stopped.")


if __name__ == "__main__":
    run(interval=1)
