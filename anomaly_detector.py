"""
Anomaly detector: reads sensor-readings, applies rules, publishes alerts
to the sensor-alerts topic.

Rules:
  - temperature not in [TEMP_MIN, TEMP_MAX]   -> TEMP_HIGH / TEMP_LOW
  - humidity    not in [HUM_MIN, HUM_MAX]     -> HUMIDITY_HIGH / HUMIDITY_LOW
  - device silent for > SILENCE_SECONDS       -> SENSOR_SILENT
  - back-to-normal after active alert         -> RECOVERY (for that type)

Alerts are suppressed for COOLDOWN_SECONDS per (device, type) to avoid spam.
"""

import json
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# ---------------- Config ----------------
KAFKA_SERVERS   = os.environ.get("KAFKA_SERVERS", "localhost:9092")
IN_TOPIC        = os.environ.get("KAFKA_TOPIC", "sensor-readings")
OUT_TOPIC       = os.environ.get("ALERTS_TOPIC", "sensor-alerts")
GROUP_ID        = os.environ.get("DETECTOR_GROUP", "anomaly-detector-group")

TEMP_MIN, TEMP_MAX = 2.0, 8.0
HUM_MIN,  HUM_MAX  = 40.0, 80.0
SILENCE_SECONDS    = 15
COOLDOWN_SECONDS   = 300     # 5 minutes per (device, anomaly_type)
SILENCE_CHECK_EVERY = 3      # check for silent devices every 3 sec

# ---------------- Shutdown handling ----------------
_shutdown = {"flag": False}
def _stop(signum, frame):
    print("[signal] " + str(signum) + " shutting down...")
    _shutdown["flag"] = True
signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)

# ---------------- State ----------------
last_seen = {}                         # device_id -> epoch seconds
active_alerts = {}                     # (device_id, type) -> last_alert_epoch
last_silence_check = 0.0

# ---------------- Helpers ----------------
def now_utc():
    return datetime.now(timezone.utc)

def build_alert(reading, anomaly_type, severity, value, threshold, message):
    return {
        "alert_id": str(uuid.uuid4()),
        "detected_at": now_utc().isoformat(),
        "device_id": reading.get("device_id"),
        "client":    reading.get("client"),
        "site":      reading.get("site"),
        "room_type": reading.get("room_type"),
        "anomaly_type": anomaly_type,
        "severity":     severity,
        "value":        value,
        "threshold":    threshold,
        "message":      message,
        "reading_timestamp": reading.get("timestamp"),
    }

def should_emit(device_id, anomaly_type, ts):
    key = (device_id, anomaly_type)
    last = active_alerts.get(key)
    if last is None:
        active_alerts[key] = ts
        return True
    if ts - last >= COOLDOWN_SECONDS:
        active_alerts[key] = ts
        return True
    return False

def clear_active(device_id, anomaly_type, ts):
    key = (device_id, anomaly_type)
    if key in active_alerts:
        del active_alerts[key]
        return True
    return False

def check_value_rules(reading):
    """Return list of (anomaly_type, severity, value, threshold, message) tuples."""
    alerts = []
    did = reading.get("device_id")
    t   = reading.get("temperature")
    h   = reading.get("humidity")
    ts  = now_utc().timestamp()

    # Temperature
    if isinstance(t, (int, float)):
        if t > TEMP_MAX:
            if should_emit(did, "TEMP_HIGH", ts):
                alerts.append(("TEMP_HIGH", "CRITICAL", t,
                               "<=" + str(TEMP_MAX),
                               did + " at " + str(reading.get("site")) +
                               ": temperature " + str(t) + "C exceeds " + str(TEMP_MAX) + "C"))
        elif t < TEMP_MIN:
            if should_emit(did, "TEMP_LOW", ts):
                alerts.append(("TEMP_LOW", "CRITICAL", t,
                               ">=" + str(TEMP_MIN),
                               did + " at " + str(reading.get("site")) +
                               ": temperature " + str(t) + "C below " + str(TEMP_MIN) + "C"))
        else:
            if clear_active(did, "TEMP_HIGH", ts):
                alerts.append(("RECOVERY", "INFO", t, "in-range",
                               did + " temperature back to normal (" + str(t) + "C)"))
            if clear_active(did, "TEMP_LOW", ts):
                alerts.append(("RECOVERY", "INFO", t, "in-range",
                               did + " temperature back to normal (" + str(t) + "C)"))

    # Humidity
    if isinstance(h, (int, float)):
        if h > HUM_MAX:
            if should_emit(did, "HUMIDITY_HIGH", ts):
                alerts.append(("HUMIDITY_HIGH", "WARNING", h,
                               "<=" + str(HUM_MAX),
                               did + " at " + str(reading.get("site")) +
                               ": humidity " + str(h) + "% exceeds " + str(HUM_MAX) + "%"))
        elif h < HUM_MIN:
            if should_emit(did, "HUMIDITY_LOW", ts):
                alerts.append(("HUMIDITY_LOW", "WARNING", h,
                               ">=" + str(HUM_MIN),
                               did + " at " + str(reading.get("site")) +
                               ": humidity " + str(h) + "% below " + str(HUM_MIN) + "%"))
        else:
            if clear_active(did, "HUMIDITY_HIGH", ts):
                alerts.append(("RECOVERY", "INFO", h, "in-range",
                               did + " humidity back to normal (" + str(h) + "%)"))
            if clear_active(did, "HUMIDITY_LOW", ts):
                alerts.append(("RECOVERY", "INFO", h, "in-range",
                               did + " humidity back to normal (" + str(h) + "%)"))

    return alerts

# ---------------- Main ----------------
def main():
    global last_silence_check

    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_SERVERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )

    print("[detector] listening on '" + IN_TOPIC + "' -> '" + OUT_TOPIC + "'")
    print("[detector] rules: temp [" + str(TEMP_MIN) + "," + str(TEMP_MAX) + "]C  "
          "humidity [" + str(HUM_MIN) + "," + str(HUM_MAX) + "]%  "
          "silence>" + str(SILENCE_SECONDS) + "s  cooldown=" + str(COOLDOWN_SECONDS) + "s")

    emitted = 0

    while not _shutdown["flag"]:
        try:
            batch = consumer.poll(timeout_ms=1000)
        except KafkaError as e:
            print("[detector] kafka error: " + str(e))
            time.sleep(2)
            continue

        for tp, messages in batch.items():
            for msg in messages:
                reading = msg.value
                did = reading.get("device_id")
                if did:
                    last_seen[did] = now_utc().timestamp()

                for atype, sev, val, thr, text in check_value_rules(reading):
                    ev = build_alert(reading, atype, sev, val, thr, text)
                    producer.send(OUT_TOPIC, key=did or "unknown", value=ev)
                    emitted += 1
                    print("[detector] " + atype + " " + str(did) + ": " + text)

        if batch:
            try:
                consumer.commit()
            except KafkaError as e:
                print("[detector] commit failed: " + str(e))

        # Silence check (runs every SILENCE_CHECK_EVERY seconds)
        now_ts = now_utc().timestamp()
        if now_ts - last_silence_check >= SILENCE_CHECK_EVERY:
            last_silence_check = now_ts
            for did, seen_at in list(last_seen.items()):
                if now_ts - seen_at > SILENCE_SECONDS:
                    if should_emit(did, "SENSOR_SILENT", now_ts):
                        ev = {
                            "alert_id": str(uuid.uuid4()),
                            "detected_at": now_utc().isoformat(),
                            "device_id": did,
                            "client": None, "site": None, "room_type": None,
                            "anomaly_type": "SENSOR_SILENT",
                            "severity": "CRITICAL",
                            "value": int(now_ts - seen_at),
                            "threshold": SILENCE_SECONDS,
                            "message": did + " has not reported for " +
                                       str(int(now_ts - seen_at)) + " seconds",
                            "reading_timestamp": None,
                        }
                        producer.send(OUT_TOPIC, key=did, value=ev)
                        emitted += 1
                        print("[detector] SENSOR_SILENT " + did)

    producer.flush(timeout=10)
    producer.close()
    consumer.close()
    print("[detector] stopped, emitted " + str(emitted) + " alerts")


if __name__ == "__main__":
    main()
