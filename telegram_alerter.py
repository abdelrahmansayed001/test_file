"""
Telegram alerter: reads sensor-alerts, sends formatted messages to a Telegram group.
"""
import json
import os
import signal
import sys
import time

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_SERVERS  = os.environ.get("KAFKA_SERVERS", "localhost:9092")
ALERTS_TOPIC   = os.environ.get("ALERTS_TOPIC", "sensor-alerts")
GROUP_ID       = os.environ.get("TELEGRAM_GROUP", "telegram-alerter-group")

BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID")

SEVERITY_ICON = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🟢"}

_shutdown = {"flag": False}
def _stop(signum, frame):
    print("[telegram] signal " + str(signum) + " shutting down...")
    _shutdown["flag"] = True
signal.signal(signal.SIGINT,  _stop)
signal.signal(signal.SIGTERM, _stop)


def format_alert(a):
    icon  = SEVERITY_ICON.get(a.get("severity", ""), "⚪")
    atype = a.get("anomaly_type", "UNKNOWN")
    did   = a.get("device_id", "?")
    site  = a.get("site") or "-"
    cli   = a.get("client") or "-"
    rt    = a.get("room_type") or "-"

    title = atype.replace("_", " ").title()
    if atype == "RECOVERY":
        icon = "🟢"

    lines = [
        icon + " <b>" + title + "</b>",
        "Device: <code>" + str(did) + "</code> (" + str(cli) + " / " + str(site) + " / " + str(rt) + ")",
    ]
    if a.get("value") is not None and a.get("anomaly_type") != "SENSOR_SILENT":
        lines.append("Value: <b>" + str(a.get("value")) + "</b>   Threshold: " + str(a.get("threshold")))
    if a.get("anomaly_type") == "SENSOR_SILENT":
        lines.append("Silent for <b>" + str(a.get("value")) + "s</b> (threshold " + str(a.get("threshold")) + "s)")
    lines.append("Time: <code>" + str(a.get("detected_at")) + "</code>")
    return "\n".join(lines)


def send_telegram(text, retries=3):
    if not BOT_TOKEN or not CHAT_ID:
        print("[telegram] ERROR: TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set")
        return False
    url = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(1, retries + 1):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.ok:
                return True
            print("[telegram] HTTP " + str(r.status_code) + " " + r.text[:200])
        except requests.RequestException as e:
            print("[telegram] attempt " + str(attempt) + "/" + str(retries) + " failed: " + str(e))
        time.sleep(2)
    return False


def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("[telegram] set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID first")
        sys.exit(1)

    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=KAFKA_SERVERS.split(","),
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    print("[telegram] listening on '" + ALERTS_TOPIC + "' -> chat " + str(CHAT_ID))
    sent = 0

    try:
        for msg in consumer:
            if _shutdown["flag"]:
                break
            alert = msg.value
            text = format_alert(alert)
            ok = send_telegram(text)
            if ok:
                consumer.commit()
                sent += 1
                print("[telegram] sent " + alert.get("anomaly_type", "?") +
                      " for " + str(alert.get("device_id")))
            else:
                print("[telegram] FAILED to send " + str(alert.get("alert_id")) +
                      " - offset not committed, will retry on restart")
                time.sleep(5)
    except KafkaError as e:
        print("[telegram] kafka error: " + str(e))
    finally:
        consumer.close()
        print("[telegram] stopped, sent " + str(sent) + " messages")


if __name__ == "__main__":
    main()
