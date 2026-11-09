# IoT Sensor Pipeline - Project Documentation

## 1. Executive Summary

Temperature-sensitive products (vaccines, food, chemicals) live in refrigerated rooms. If a fridge drifts out of the safe 2-8 C range overnight and nobody notices, thousands of dollars of product can be ruined. This project makes every reading visible in real time: five simulated sensors report every second, every reading is validated, sent through Kafka as a durable buffer, and permanently stored in Aiven's cloud PostgreSQL. Any reading can be queried from anywhere within seconds.

Key results: 3000+ readings stored, zero data loss, one command to launch.

## 2. Architecture

Generator -> Flask API -> Kafka -> Consumer -> Aiven PostgreSQL

- Generator (sensor_generator_http.py): builds 5 readings per second and POSTs them to the API
- Flask API (kafka_api_flask.py): validates each reading and publishes it to Kafka
- Kafka: durable message queue; messages survive broker or consumer crashes
- Consumer (kafka_consumer_aiven.py): batches messages (up to 200 or 2s), INSERTs into Aiven, then commits the Kafka offset (at-least-once delivery)
- Aiven PostgreSQL: cloud table readings, queryable from any machine with credentials

## 3. Files and Their Roles

| File | Role |
|---|---|
| Mostafa_work.py | Device list + realistic reading generator |
| kafka_api_flask.py | HTTP API (POST /readings, POST /readings/batch, GET /health) |
| sensor_generator_http.py | Continuous generator: POSTs to API |
| kafka_consumer_aiven.py | Kafka -> Aiven consumer |
| run_pipeline.ps1 | One-command launcher |
| stop_pipeline.ps1 | Clean stopper |
| requirements.txt | Exact dependency versions |
| PROJECT_DOCUMENTATION.md | This file |

## 4. Quick Start

Prerequisites: Windows 10/11, Python 3.10-3.14, Docker Desktop, an Aiven PostgreSQL database.

One-time setup:

    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
    $env:AIVEN_URL = "postgres://avnadmin:PASSWORD@HOST:PORT/defaultdb?sslmode=require"
    .\run_pipeline.ps1

run_pipeline.ps1 auto-creates the venv, installs requirements.txt, starts Docker if needed, creates the Kafka container if missing, and launches all three processes.

Watch live:    Get-Content logs\consumer.log -Wait -Tail 20
Stop:          .\stop_pipeline.ps1

## 5. Data Format

Every reading looks like this JSON:

    {
      "timestamp": "2026-10-22T15:47:39.123456+00:00",
      "device_id": "DEV001",
      "client": "Carffor",
      "site": "Cairo",
      "room_type": "Cold Storage",
      "temperature": 5.1,
      "humidity": 61.3
    }

Stored in Aiven table readings with columns: id, ts, device_id, client, site, room_type, temperature, humidity, kafka_offset, ingested_at.

## 6. Results

| Metric | Value |
|---|---|
| Devices | 5 |
| Rate | ~5 readings/second |
| Total stored | 3000+ |
| End-to-end latency | under 3 seconds |
| Data loss | 0 |

## 7. Troubleshooting

| Symptom | Fix |
|---|---|
| API did not respond | Check logs\api.err.log |
| Kafka not ready | docker start kafka-iot |
| Aiven connection failed | Check AIVEN_URL, whitelist your IP in Aiven |
| Logs empty | Ensure launcher uses -u flag |
| WARNING: expected 3 scripts | Cosmetic - venv shim doubles PIDs |

## 8. Appendix A - Tools and Versions

### Runtime requirements (install manually)

| Tool | Version | Where to get |
|---|---|---|
| Windows | 10 or 11 | default |
| PowerShell | 5.1+ | built in |
| Python | 3.10 - 3.14 (tested 3.14.2) | python.org |
| Docker Desktop | 24 or later | docker.com |
| PostgreSQL | 14+ (hosted on Aiven) | aiven.io |
| Aiven account | free tier works | aiven.io |

### Python packages (installed automatically by run_pipeline.ps1)

| Package | Version | Purpose |
|---|---|---|
| flask | 3.0.3 | web API |
| werkzeug | 3.0.4 | Flask dependency |
| kafka-python | 2.0.2 | Kafka client |
| psycopg2-binary | 2.9.9 | PostgreSQL driver |
| requests | 2.32.3 | HTTP client |
| pandas | 2.2.3 | tabular data |
| numpy | 2.1.3 | numeric support |
| python-dateutil | 2.9.0.post0 | datetime parsing |
| pytz | 2024.2 | timezone handling |

### Docker images

| Image | Tag | Purpose |
|---|---|---|
| apache/kafka | 3.7.0 | Kafka broker |

### Kafka topic

| Name | Partitions | Replication |
|---|---|---|
| sensor-readings | 3 | 1 |

## 9. Appendix B - Running on Another Machine

What you need:
1. The ZIP extracted to any folder (for example C:\iot_project)
2. Python 3.10-3.14 installed
3. Docker Desktop installed and started (whale icon steady)
4. AIVEN_URL exported in the session

Steps:

    cd C:\iot_project
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
    $env:AIVEN_URL = "postgres://avnadmin:PASSWORD@HOST:PORT/defaultdb?sslmode=require"
    .\run_pipeline.ps1

The launcher handles: venv creation, pip install from requirements.txt, Docker Desktop start, Kafka container creation, Kafka topic creation, launching API+consumer+generator, health checks.

Verify data is flowing:    Get-Content logs\consumer.log -Wait -Tail 20
Stop:                       .\stop_pipeline.ps1

## 10. Appendix C - Error Reference

### C.1 - running scripts is disabled on this system
Cause: PowerShell execution policy blocks .ps1 files.
Fix: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

### C.2 - Python was not found
Cause: Python not installed, or "Add to PATH" not checked.
Fix: Reinstall Python from python.org, check "Add Python to PATH", reopen PowerShell, verify with: python --version

### C.3 - ModuleNotFoundError: No module named requests
Cause: requirements not installed in the venv.
Fix: Rerun .\run_pipeline.ps1 - the launcher installs requirements automatically. If it persists: .\.venv\Scripts\python.exe -m pip install -r requirements.txt

### C.4 - Docker Desktop not running
Cause: Docker is installed but not started.
Fix: Open Docker Desktop from the Start menu, wait for the whale icon to stop animating (30-60 seconds), rerun .\run_pipeline.ps1

### C.5 - container kafka-iot not found
Cause: first run on a new machine - the container has not been created yet.
Fix: Rerun .\run_pipeline.ps1 - the launcher creates the container if missing (about 30 seconds the first time).

### C.6 - Kafka not ready
Cause: the Kafka broker inside the container has not finished bootstrapping.
Fix: Wait 20-30 seconds and rerun. Or check: docker logs kafka-iot --tail 30

### C.7 - AIVEN_URL is not set
Cause: the environment variable was not exported in this PowerShell session.
Fix: $env:AIVEN_URL = "postgres://avnadmin:PASSWORD@HOST:PORT/defaultdb?sslmode=require"
Permanent (across sessions): [Environment]::SetEnvironmentVariable("AIVEN_URL","postgres://...","User") then close and reopen PowerShell.

### C.8 - FATAL: password authentication failed for user avnadmin
Cause: wrong password in AIVEN_URL.
Fix: Aiven console -> your service -> Users -> avnadmin -> copy connection URI or reset password. Update $env:AIVEN_URL and rerun.

### C.9 - no pg_hba.conf entry for host
Cause: Aiven is blocking your IP address.
Fix: Aiven console -> your service -> Overview -> Allowed IP addresses -> Add current IP (or 0.0.0.0/0 for a demo only). Save. Changes take about 30 seconds.

### C.10 - SSL SYSCALL error: EOF detected
Cause: network or firewall is blocking the SSL connection to Aiven.
Fix: Confirm port 14544 is open outbound. Try from a phone hotspot (rules out corporate firewall). Check VPN is not intercepting SSL.

### C.11 - could not translate host name
Cause: DNS cannot resolve the Aiven host.
Fix: Check internet. Try nslookup pg-xxx.aivencloud.com. Disable VPN briefly to test.

### C.12 - API did not respond
Cause: Flask crashed at startup - usually a missing module or a port already in use.
Fix: Read the exact error in logs\api.err.log:
    Get-Content logs\api.err.log -Tail 40
Common fixes:
- ModuleNotFoundError -> rerun .\run_pipeline.ps1
- Address already in use -> Get-NetTCPConnection -LocalPort 8001 | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }

### C.13 - WARNING: expected 3 python processes, found 6
Cause: Cosmetic only. The venv shim (python.exe) launches a real Python interpreter as a child, so each logical process shows two PIDs.
Fix: None needed.

### C.14 - Consumer log shows inserted rows but Aiven shows nothing
Cause: AIVEN_URL points to a different database than the one you are querying.
Fix: Print $env:AIVEN_URL and query that exact database.

### C.15 - Everything looks fine but no data flows
Fix checks, in order:
1. Get-Content logs\generator.err.log -Tail 40
2. Invoke-RestMethod http://127.0.0.1:8001/health
3. docker exec kafka-iot /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic sensor-readings
4. Get-Content logs\consumer.err.log -Tail 30

### C.16 - at most 1000 readings per request
Cause: the generator sent more than the API MAX_BATCH limit.
Fix: reduce MAX_PENDING in sensor_generator_http.py.

### C.17 - missing fields
Cause: a request was posted without the required fields.
Fix: every reading needs timestamp, device_id, client, site, temperature, humidity. room_type is optional.

### C.18 - unknown device: DEV999
Cause: device ID not in Mostafa_work.py DEVICES.
Fix: use DEV001..DEV005, or add the device to DEVICES.

### C.19 - timestamp must be in ISO format
Fix: use format 2026-10-22T15:47:39+00:00 (or ending in Z for UTC).

### C.20 - temperature must be between -50 and 100
Cause: value is outside the physical range the API accepts.
Fix: send a realistic value (2-8 for cold storage).

### C.21 - Nothing works, help
Checklist:
1. python --version works (3.10+)
2. docker info succeeds (Docker Desktop running)
3. docker ps shows kafka-iot Up
4. $env:AIVEN_URL is set and correct
5. Get-Content logs\api.err.log -Tail 30 shows nothing red
6. Get-Content logs\consumer.err.log -Tail 30 shows nothing red
Attach the output of those 6 commands to any support request.

## Support

If you hit an error not listed here, capture:
- The exact error message (screenshot or copy)
- Get-Content logs\*.log -Tail 30
- docker ps
- python --version and docker --version
Send these to the project owner.

End of documentation.
