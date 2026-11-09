"""
Kafka -> Aiven Postgres consumer.
Inserts readings from Kafka topic sensor-readings into Aiven Postgres in real time.
"""
import json
import os
import signal
import sys
import time

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import KafkaError

AIVEN_URL      = os.environ.get("AIVEN_URL","REMOVED_SECRET")
KAFKA_SERVERS  = os.environ.get("KAFKA_SERVERS", "localhost:9092")
KAFKA_TOPIC    = os.environ.get("KAFKA_TOPIC", "sensor-readings")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "aiven-consumer-group")

BATCH_SIZE    = 200
BATCH_TIMEOUT = 2.0

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS readings (
    id           BIGSERIAL PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL,
    device_id    TEXT NOT NULL,
    client       TEXT,
    site         TEXT,
    room_type    TEXT,
    temperature  DOUBLE PRECISION,
    humidity     DOUBLE PRECISION,
    kafka_offset BIGINT,
    ingested_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_readings_device_ts ON readings (device_id, ts DESC);
"""

INSERT_SQL = """
INSERT INTO readings
    (ts, device_id, client, site, room_type, temperature, humidity, kafka_offset)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s);
"""


def build_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS.split(","),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def connect_aiven(retries=10, delay=3):
    for attempt in range(1, retries + 1):
        try:
            con = psycopg2.connect(AIVEN_URL, connect_timeout=10)
            con.autocommit = False
            print("[AIVEN] connected to Aiven Postgres")
            return con
        except psycopg2.OperationalError as e:
            print(f"[AIVEN] attempt {attempt}/{retries} failed: {e}")
            time.sleep(delay)
    print("[AIVEN] could not connect, giving up")
    sys.exit(1)


def setup_schema(con):
    with con.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(CREATE_INDEX_SQL)
    con.commit()
    print("[AIVEN] schema ready.")


def insert_batch(con, records, offsets):
    rows = []
    for r, meta in zip(records, offsets):
        rows.append((
            r.get("timestamp"), r.get("device_id"), r.get("client"),
            r.get("site"), r.get("room_type"), r.get("temperature"),
            r.get("humidity"), meta.offset,
        ))
    with con.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
    con.commit()
    return len(rows)


_shutdown = {"flag": False}


def _handle_signal(signum, frame):
    print(f"[signal] {signum} received, shutting down")
    _shutdown["flag"] = True


signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def main():
    print("[Kafka] connecting consumer...")
    consumer = build_consumer()
    print("[AIVEN] connecting...")
    con = connect_aiven()
    setup_schema(con)

    print(f"[Kafka] consuming topic='{KAFKA_TOPIC}' servers={KAFKA_SERVERS} group='{KAFKA_GROUP_ID}'")

    buffer, meta_buffer = [], []
    last_flush = time.time()
    total = 0

    def flush():
        nonlocal buffer, meta_buffer, last_flush, total
        if not buffer:
            return
        try:
            n = insert_batch(con, buffer, meta_buffer)
            consumer.commit()
            total += n
            print(f"[AIVEN] inserted {n} rows (total={total})")
        except Exception as e:
            con.rollback()
            print(f"[AIVEN] insert failed, batch dropped: {e}")
        buffer, meta_buffer = [], []
        last_flush = time.time()

    try:
        for msg in consumer:
            if _shutdown["flag"]:
                break
            buffer.append(msg.value)
            meta_buffer.append(msg)
            if len(buffer) >= BATCH_SIZE or time.time() - last_flush > BATCH_TIMEOUT:
                flush()
        flush()
    except KafkaError as e:
        print(f"[Kafka] error: {e}")
    finally:
        try:
            flush()
        except Exception:
            pass
        consumer.close()
        con.close()
        print(f"[done] total inserted this session: {total}")


if __name__ == "__main__":
    main()

