
import random
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
import sys
import psycopg2

# ------------------------------------------------------------------ #
# PostgreSQL settings: change the password to the one you chose
# when you installed PostgreSQL
# ------------------------------------------------------------------ #
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "iot",          # create it first:  CREATE DATABASE iot;
    "user": "postgres",
    "password": "0000",
}
# device_id -> (client, site, room_id, room_type)
DEVICES = {
    'DEV001': {'client': 'Carffor', 'site': 'Cairo', 'room_id': 'R101', 'room_type': 'Cold Storage'},
    'DEV002': {'client': 'Carffor', 'site': 'Maddi', 'room_id': 'R102', 'room_type': 'Cold Storage'},
    'DEV003': {'client': 'Nile Foods', 'site': 'Giza', 'room_id': 'R201', 'room_type': 'Cold Storage'},
    'DEV004': {'client': 'Nile pahrmcy', 'site': 'Giza OC', 'room_id': 'R202', 'room_type': 'Lab'},
    'DEV005': {'client': 'Saudi', 'site': 'Alex', 'room_id': 'R301', 'room_type': 'Butchery'},
}
#----------------{fault}-------------#
# Device ID -> active fault information (starts empty)
faults = {}

# Probability of starting a fault on each device per cycle
FAULT_PROBABILITY = 0.01

# Fault duration:
FAULT_DURATION = timedelta(minutes=1)

# Normal sensor ranges
TEMP_MIN, TEMP_MAX = 2, 8
HUM_MIN, HUM_MAX = 40, 80

# Initial sensor conditions (temperature inside the normal range)
conditions = {
    device_id: {
        "temperature": random.uniform(TEMP_MIN, TEMP_MAX),
        "humidity": random.uniform(HUM_MIN, HUM_MAX),
    }
    for device_id in DEVICES
}


def generate_reading(device_id, info):
    state = conditions[device_id]
    now = datetime.now(timezone.utc)

    state["temperature"] += random.uniform(-0.3, 0.3)
    state["humidity"] += random.uniform(-0.8, 0.8)
    state["temperature"] = max(TEMP_MIN, min(TEMP_MAX, state["temperature"]))
    state["humidity"] = max(HUM_MIN, min(HUM_MAX, state["humidity"]))

    # Start a random fault if this device has none active
    if device_id not in faults and random.random() < FAULT_PROBABILITY:
        parameter = random.choice(["temperature", "humidity"])

        if parameter == "temperature":
            value = random.choice([
                random.uniform(TEMP_MIN - 5, TEMP_MIN - 1),
                random.uniform(TEMP_MAX + 1, TEMP_MAX + 5),
            ])
        else:
            value = random.choice([
                random.uniform(HUM_MIN - 10, HUM_MIN - 1),
                random.uniform(HUM_MAX + 1, HUM_MAX + 10),
            ])

        faults[device_id] = {
            "parameter": parameter,
            "value": round(value, 2),
            "end_time": now + FAULT_DURATION,
        }

    # Apply the fault while it is active
    temperature = state["temperature"]
    humidity = state["humidity"]
    fault = faults.get(device_id)

    if fault:
        if now < fault["end_time"]:
            if fault["parameter"] == "temperature":
                temperature = fault["value"]
            else:
                humidity = fault["value"]
        else:
            del faults[device_id]  # fault finished

    temperature = round(temperature, 2)
    humidity = round(humidity, 2)



    return {
        "timestamp": now.isoformat(),
        "device_id": device_id,
        "client": info["client"],
        "site": info["site"],
        "room_type": info["room_type"],
        "temperature": temperature,
        "humidity": humidity,

    }

def save_data(data, filename="sensor_data.csv"):
    if not data:
        print("No data to save.")
        return
    df = pd.DataFrame(data)

    if filename.endswith(".csv"):
        df.to_csv(filename, index=False)
    else:
        print("Unsupported file format.")
        return

    print(f"Saved {len(df)} readings to {filename}")

# 5. PostgreSQL helpers
CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS readings (
                        id          BIGSERIAL PRIMARY KEY,
                        ts          TIMESTAMPTZ,
                        device_id   TEXT,
                        client      TEXT,
                        site        TEXT,
                        temperature DOUBLE PRECISION,
                        humidity    DOUBLE PRECISION)"""

INSERT_SQL = """INSERT INTO readings (ts, device_id, client, site, temperature, humidity)
                VALUES (%s, %s, %s, %s, %s, %s)"""


def connect():
    try:
        con = psycopg2.connect(**PG_CONFIG)
    except psycopg2.OperationalError as e:
        print("Could not connect to PostgreSQL:\n", e)
        print("Check: 1) PostgreSQL is running  2) database 'iot' exists  "
            "3) the password in PG_CONFIG is correct")
        sys.exit(1)
    con.autocommit = True        # every INSERT is saved immediately
    return con


def main(interval=1, csv_backup="sensor_data.csv"):
    con = connect()
    cur = con.cursor()
    cur.execute(CREATE_TABLE_SQL)
    data = []

    print("IoT Sensor Simulator Started")
    print("Press Ctrl+C to stop and save data.")

    try:
        while True:
            for device_id, info in DEVICES.items():
                reading = generate_reading(device_id, info)
                cur.execute(INSERT_SQL, (reading["timestamp"], reading["device_id"],
                            reading["client"], reading["site"],
                            reading["temperature"], reading["humidity"]))
                data.append(reading)
                print(reading)
            # Convert current readings to a DataFrame
            df = pd.DataFrame(data)

            print("\nLatest readings:")
            
            print("-" * 50)


            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nSimulator stopped.")

    finally:
        cur.close()
        con.close()
        if csv_backup:
            save_data(data, csv_backup)


if __name__ == "__main__":
    main(interval=1)