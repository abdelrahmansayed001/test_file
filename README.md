# IoT Sensor Pipeline

Real-time IoT pipeline: sensors -> HTTP API -> Kafka -> Aiven PostgreSQL.

## Files

- Mostafa_work.py          - sensor logic + reading generator
- kafka_api_flask.py       - Flask API (validates, publishes to Kafka)
- sensor_generator_http.py - continuous generator (POSTs to API)
- kafka_consumer_aiven.py  - reads Kafka, writes to Aiven
- run_pipeline.ps1         - one-command launcher
- stop_pipeline.ps1        - clean stopper
- requirements.txt         - pinned Python packages
- PROJECT_DOCUMENTATION.md - full docs (read first)

## Prerequisites

- Windows 10/11, PowerShell 5.1+
- Python 3.10-3.14 (from python.org, "Add to PATH" checked)
- Docker Desktop (started, whale icon steady)
- An Aiven PostgreSQL database

## Run

    cd <folder where you unzipped>
    Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
    $env:AIVEN_URL = "postgres://avnadmin:PASSWORD@HOST:PORT/defaultdb?sslmode=require"
    .\run_pipeline.ps1

The launcher auto-creates the venv, installs requirements.txt, starts Docker, creates the Kafka container, and launches all three processes.

## Watch

    Get-Content logs\consumer.log -Wait -Tail 20

## Stop

    .\stop_pipeline.ps1

For errors, see PROJECT_DOCUMENTATION.md Appendix C.
