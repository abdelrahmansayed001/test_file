"""
Simple Flask API: receives readings over HTTP and publishes them to Kafka.
Built on simulator.py (keep both files in the same folder).

    pip install flask kafka-python pandas psycopg2-binary
    python kafka_api_flask.py          # http://localhost:8001
"""
import json
import os
import threading
from datetime import datetime

from flask import Flask, jsonify, request
from kafka import KafkaProducer
from kafka.errors import KafkaError

import simulator as sensors          # uses DEVICES (and the same readings format)

KAFKA_SERVERS = os.environ.get("KAFKA_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
MAX_BATCH = 1000

REQUIRED = ("timestamp", "device_id", "client", "site", "temperature", "humidity")
ALLOWED = REQUIRED + ("room_type",)

app = Flask(__name__)


# ------------------------------------------------------------------ #
# Validation: bad readings are rejected before they reach Kafka
# ------------------------------------------------------------------ #
def validate(r):
    """Return an error message, or None if the reading is valid."""
    if not isinstance(r, dict):
        return "each reading must be a JSON object"
    missing = [f for f in REQUIRED if f not in r]
    if missing:
        return f"missing fields: {', '.join(missing)}"
    if r["device_id"] not in sensors.DEVICES:
        return f"unknown device: {r['device_id']}"
    try:
        datetime.fromisoformat(str(r["timestamp"]).replace("Z", "+00:00"))
    except ValueError:
        return "timestamp must be in ISO format, e.g. 2026-10-17T11:24:55+00:00"
    for field, low, high in (("temperature", -50, 100), ("humidity", 0, 100)):
        v = r[field]
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            return f"{field} must be a number"
        if not low <= v <= high:
            return f"{field} must be between {low} and {high}"
    return None


# ------------------------------------------------------------------ #
# One Kafka producer, created once and shared by all requests
# ------------------------------------------------------------------ #
_producer = None
_lock = threading.Lock()


def get_producer():
    global _producer
    with _lock:
        if _producer is None:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",              # wait until Kafka has safely stored the message
                retries=5,
            )
        return _producer


def publish(readings):
    """Send readings to Kafka and wait for confirmation of each one."""
    producer = get_producer()
    # device_id as key: all readings of one device stay in order (same partition)
    futures = [producer.send(KAFKA_TOPIC, key=r["device_id"],
                value={k: r[k] for k in ALLOWED if k in r})
                for r in readings]
    for f in futures:
        f.get(timeout=10)


def handle(readings):
    """Validate everything, publish everything, answer 202 / 400 / 503."""
    for i, r in enumerate(readings):
        error = validate(r)
        if error:
            return jsonify(error=error, index=i), 400
    try:
        publish(readings)
    except KafkaError as e:
        return jsonify(error=f"Kafka is not available: {e}"), 503
    return jsonify(status="accepted", count=len(readings)), 202


# ------------------------------------------------------------------ #
# Endpoints
# ------------------------------------------------------------------ #
@app.get("/health")
def health():
    try:
        kafka_ok = bool(get_producer().bootstrap_connected())
    except KafkaError:
        kafka_ok = False
    return jsonify(api="ok", kafka=kafka_ok, topic=KAFKA_TOPIC, servers=KAFKA_SERVERS)


@app.post("/readings")
def post_reading():
    """Accept ONE reading (a JSON object) and publish it to Kafka."""
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify(error="send ONE reading as a JSON object"), 400
    return handle([body])


@app.post("/readings/batch")
def post_batch():
    """Accept a LIST of readings (e.g. one round from all devices)."""
    body = request.get_json(silent=True)
    if not isinstance(body, list) or not body:
        return jsonify(error="send a non-empty JSON list of readings"), 400
    if len(body) > MAX_BATCH:
        return jsonify(error=f"at most {MAX_BATCH} readings per request"), 400
    return handle(body)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8001)
