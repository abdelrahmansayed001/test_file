# Real-Time IoT Data Pipeline - Project Proposal

**Course:** Data Engineering - Round 4 (Module 5: Data Pipelines + Module 6: Big Data Processing)
**Submission Date:** October 30, 2026
**Team:** Group 5
**Course Duration:** October 9, 2026 to November 29, 2026 (Execution) - December 4-10, 2026 (Discussion)

## Document Control

| Field | Value |
|---|---|
| Document Name | Project Proposal |
| Version | 1.0 |
| Status | Draft for Review |
| Author | Rana Shaaban Abdelqader Mohamed (Team Lead) |
| Contributors | Shahd Safwat, Waleed Hamouda, Mostafa Ahmed, Abdelrahman Sayed |
| Reviewers | Course Instructor |
| Last Updated | October 30, 2026 |

**Revision History**

| Version | Date | Author | Summary |
|---|---|---|---|
| 1.0 | 2026-10-30 | Rana S. Abdelqader | Initial proposal |

## 1. Background & Motivation

Temperature-sensitive products - vaccines, pharmaceuticals, dairy, and fresh food - must be stored within strict temperature and humidity ranges (typically 2 to 8 C for cold storage, less than 60 percent humidity for pharmaceuticals). A silent drift outside these ranges, if undetected for even a few hours, can spoil entire batches and cause thousands of dollars in losses.

In practice, cold-chain failures are usually discovered hours after they happen - often the next morning - because legacy monitoring relies on periodic manual checks or on logging systems that store data but never surface it. The gap is not the sensor; the gap is the pipeline that turns raw sensor output into real-time, actionable insight.

This project addresses that gap in an educational setting: we build an end-to-end data pipeline that simulates an IoT sensor fleet, ingests its readings in real time, processes them for both batch analytics and streaming alerts, and visualizes the results in a live dashboard. The scope is intentionally modeled on real cold-chain systems but uses only simulated data - no real clients or production credentials are involved.

## 2. Problem Statement

Modern cold-chain operators face three distinct data problems, each requiring a different engineering solution:

1. Ingestion at scale. Five sensors reporting every five seconds produce 86,400 readings per day. Scaling to a real fleet of thousands of sensors makes direct database writes infeasible. A durable, high-throughput buffer is required between the sensors and the storage layer.
2. Two analytical needs from one data stream. Operators need historical, aggregated analytics (e.g., monthly temperature variance per site) that is best served by a batch pipeline into a data warehouse, and they need sub-second alerting (e.g., fridge DEV003 just exceeded 8 C) that is only achievable through streaming analytics.
3. Visibility. Even if data is collected and processed correctly, it must be surfaced to non-technical operators - site managers, quality officers - in a form they can act on within seconds.

This project will deliver a working solution to all three problems using a combination of Kafka (ingestion), Aiven PostgreSQL (real-time store), Snowflake (batch warehouse via Airflow and dbt), Azure Stream Analytics (real-time alerting), and Power BI (dashboards).

## 3. Objectives (SMART)

| ID | Objective | How Measured | Deadline |
|---|---|---|---|
| O1 | Build a Python sensor generator emitting realistic readings at 5-second intervals for 5 devices, including controlled anomaly injection. | Generator produces at least 5 readings per 5 seconds; anomaly events triggered on schedule. | Week 2 (Oct 22) |
| O2 | Ingest readings into a Kafka topic with zero data loss through broker restarts. | Kafka offsets equal committed messages equal DB row count, verified end-to-end. | Week 2 (Oct 22) - COMPLETE |
| O3 | Build a batch ETL pipeline that extracts historical CSV data, transforms it with dbt, and loads into Snowflake, orchestrated by Airflow. | Scheduled DAG runs successfully; Snowflake tables populated; dbt tests pass. | Week 6 (Nov 19) |
| O4 | Build a streaming analytics pipeline using Azure Stream Analytics that triggers alerts within 10 seconds of a threshold breach. | Alert generated for every injected anomaly; false-negative rate is zero. | Week 7 (Nov 26) |
| O5 | Deliver a Power BI dashboard with three views (live status, alerts, historical trends) refreshing every 30 seconds. | Dashboard live-demoed; all metrics from Document 5 visible. | Week 7 (Nov 26) |
| O6 | Document the entire system in a final PDF report covering architecture, results, KPIs, and lessons learned. | Report submitted to LMS before Dec 3. | Week 8 (Dec 3) |

## 4. Scope

### 4.1 In Scope

- Data simulation: 5 virtual devices (DEV001-DEV005) across Cairo, Giza, and Alexandria, generating temperature and humidity readings at 5-second intervals, with a configurable anomaly injector.
- Ingestion: Python to Flask HTTP API to Apache Kafka (topic sensor-readings).
- Real-time storage: Kafka to Python consumer to Aiven PostgreSQL (streaming sink).
- Batch warehouse: CSV exports and historical snapshots to Airflow (orchestration) to dbt (transformation) to Snowflake (warehouse).
- Streaming analytics: Kafka to Azure Stream Analytics to alert events.
- Visualization: Power BI dashboard reading from both Aiven PostgreSQL (real-time) and Snowflake (historical).
- Operational tooling: Docker Compose for Kafka, PowerShell launcher scripts, logging and monitoring.
- Documentation: Project Proposal, Plan, Task Assignments, Risk Register, KPI document, Final Report.

### 4.2 Out of Scope

- Real hardware sensors - the project simulates devices; no physical IoT hardware is procured.
- Production deployment - the environment is a controlled demo, not a public-facing production system.
- User authentication and multi-tenancy - the API and dashboard are single-tenant and open within the local network.
- Mobile application - dashboard is web-only.
- Paid tier cloud resources - all cloud usage stays within free tiers (Aiven Free, Azure Student, Power BI Desktop).
- Real client engagement - the clients (Carffor, Nile Foods, etc.) are fictional, used for realistic device attribution only.

## 5. Stakeholders

| Stakeholder | Role | Interest |
|---|---|---|
| Course Instructor | Evaluator, milestone reviewer | Correctness, completeness, professional documentation |
| Team Lead (Rana) | Project owner | On-time delivery, quality, team coordination |
| Team Members | Contributors | Learning, portfolio-quality output |
| Simulated Clients (Carffor, Nile Foods, Nile Pharmacy, Saudi) | Fictional end-users | Represent the business context and dashboard audience |
| Future Students | Handover audience | Well-documented, reproducible project |

## 6. Success Criteria

| # | Criterion | Verification Method |
|---|---|---|
| SC1 | All 4 milestone deliverables from the course PDF are demoed live | Instructor observes demo |
| SC2 | End-to-end latency (sensor to dashboard) less than 5 seconds at p95 | Timestamped readings tracked through pipeline |
| SC3 | Zero data loss - Kafka offset count equals Aiven row count | Automated verification script |
| SC4 | Alerts fire within 10 seconds of a simulated threshold breach | Inject known anomaly, measure alert timestamp |
| SC5 | Batch pipeline runs successfully end-to-end via Airflow | DAG run log and Snowflake row counts |
| SC6 | Power BI dashboard displays all 6 KPIs from Document 5 | Visual review |
| SC7 | Pipeline runs continuously for 4 hours with no manual intervention | Watch logs, monitor availability |
| SC8 | Final PDF report submitted to LMS before Dec 3 | LMS submission confirmation |

## 7. High-Level Architecture

The system is intentionally two-headed: one stream handles the now (real-time alerts and live dashboards), and the other handles the then (historical analytics and trend reporting). Both consume from the same underlying Kafka topic, which is the single source of truth.

Generator to Flask API to Kafka topic sensor-readings

STREAMING PATH:
Kafka to Python Consumer to Aiven PostgreSQL (real-time store) to Azure Stream Analytics to alert events

BATCH PATH:
Kafka to CSV export and snapshots to Apache Airflow (orchestration) to dbt (transformation) to Snowflake (warehouse)

Both paths feed:
Power BI Dashboard - Live status, Alerts, Historical trends

Design principle: Kafka acts as a durable buffer, so no reading is ever lost if any downstream component goes down. The two analytical paths can be developed, tested, and deployed independently.

## 8. Deliverables Summary

| Milestone | Course Deliverable | Our Deliverable |
|---|---|---|
| M1 - Data Simulation | Python generator and sample logs | Mostafa_work.py, sensor_generator_http.py, CSV sample logs, anomaly injector |
| M2 - Batch ETL | ETL script or pipeline and processed data | Airflow DAG, dbt models, Snowflake schema and loaded tables, batch verification report |
| M3 - Streaming Analytics | Streaming pipeline and alert code/output | Kafka topic, Python consumer, Azure Stream Analytics job, alert rules, alert log samples |
| M4 - Dashboard and Report | Dashboard and final report | Power BI .pbix file and screenshots, final PDF report (~15 pages) |

Plus the planning deliverables: Project Proposal (this document), Project Plan, Task Assignments and Roles, Risk Assessment and Mitigation Plan, KPI Document.

## 9. Assumptions & Constraints

### 9.1 Assumptions

1. All team members have access to a Windows or Linux machine with Docker Desktop installed.
2. An Aiven PostgreSQL free-tier instance is available and remains within free-tier limits (5 GB storage, 1 GB RAM).
3. Azure for Students credits cover Azure Stream Analytics usage for the project duration.
4. Snowflake trial credits remain sufficient for the 6-week batch pipeline demo.
5. Power BI Desktop is available (free) for all team members.
6. Internet connectivity is stable during demo hours (Aiven and Azure are cloud-hosted).
7. Instructor feedback after each milestone arrives within 3 working days so we can incorporate it before the next milestone.

### 9.2 Constraints

1. Time: 7.3 weeks of execution (Oct 9 to Nov 29), plus a buffer week (Nov 29 to Dec 3) and discussion window (Dec 4-10).
2. Budget: zero - all tools must fit within free tiers or academic licenses.
3. Team: All 5 members are also enrolled in other courses; availability is limited to about 10-12 hours per week per member outside class hours.
4. Environment: Mixed Windows and Linux development - the pipeline must be reproducible on both (hence Docker for Kafka, and PowerShell plus bash launchers).
5. Instructor review gates: Each milestone requires an instructor review before the next starts - no milestone can slip without cascading.

## 10. Timeline Snapshot

Full Gantt chart appears in the Project Plan document.

| Week | Dates | Focus | Milestone Status |
|---|---|---|---|
| W1 | Oct 9 to Oct 15 | Kickoff, environment setup, Kafka and Aiven working | - |
| W2 | Oct 16 to Oct 22 | Generator, Flask API, first end-to-end flow | M1 delivered |
| W3 | Oct 23 to Oct 29 | Streaming consumer stabilization, logging | - |
| W4 | Oct 30 to Nov 5 | Snowflake schema, dbt models, Airflow DAG skeleton | Proposal submitted |
| W5 | Nov 6 to Nov 12 | Batch ETL end-to-end, instructor review | M2 delivered |
| W6 | Nov 13 to Nov 19 | Azure Stream Analytics, alert rules, thresholds | - |
| W7 | Nov 20 to Nov 26 | Power BI dashboard, KPI implementation | M3 delivered |
| W8 | Nov 27 to Dec 3 | Integration hardening, final report writing | M4 delivered |
| Buffer | Dec 4 to Dec 10 | Demo prep, Q&A rehearsal | Discussion |

## 11. Approval

| Role | Name | Signature | Date |
|---|---|---|---|
| Team Lead | Rana Shaaban Abdelqader Mohamed | __________________ | __________ |
| Team Member | Shahd Safwat Mohamed | __________________ | __________ |
| Team Member | Waleed Hamouda Abd Elfattah Mahmoud | __________________ | __________ |
| Team Member | Mostafa Ahmed Mahmoud Shaban | __________________ | __________ |
| Team Member | Abdelrahman Sayed Taha Morsi | __________________ | __________ |
| Instructor | __________________ | __________________ | __________ |

End of Proposal - Document 1 of 5
