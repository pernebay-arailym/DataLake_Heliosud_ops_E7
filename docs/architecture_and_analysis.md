# Section 3.1 & 3.2 - Data Analysis and Data Lake Architecture Design

## 1. Data Source Analysis (Volume, Velocity, Variety)

To establish an optimal Data Lake architecture for HélioSud, each incoming data source is evaluated against the 3 Vs of Big Data:

### 1.1 Inverter Telemetry
- **Volume:** ~12 million messages per day (~1.7 GB/day in production). Scale down for local testing.
- **Velocity:** High real-time velocity. Inverters emit metrics every minute continuously.
- **Variety:** Semi-structured JSON streams. Multi-vendor setup presents schema variations (e.g., vendor-specific fault code fields, unit representations, and timestamp formats).
- **Architectural Implication:** Requires a high-throughput, low-latency message broker (e.g., Apache Kafka / Redpanda) to buffer stream bursts before persisting micro-batches to storage.

### 1.2 Smart Meter Load Curves (Consumption & Production)
- **Volume:** ~600,000 records daily across 12,000 households (30-minute intervals).
- **Velocity:** Batch processing on a daily scheduled cadence.
- **Variety:** Structured CSV files containing fine-grained timestamped power figures.
- **Regulatory / Architectural Implication:** Contains sensitive personal data revealing household occupancy and lifestyle habits. Requires strict access control, encryption, and GDPR compliance workflows.

### 1.3 Household Contracts
- **Volume:** ~12,000 records.
- **Velocity:** Batch processing (daily snapshot / delta updates).
- **Variety:** Structured CSV containing client IDs, contract details, and address metadata.
- **Regulatory / Architectural Implication:** Contains direct Personal Identifiable Information (PII). Access must be restricted to authorized roles only.

### 1.4 Weather Data
- **Volume:** ~1,000 files/day across 40 solar plant locations.
- **Velocity:** Micro-batch processing on an hourly cadence.
- **Variety:** Semi-structured CSV / JSON containing solar irradiance, cloud cover, and ambient temperature readings.
- **Architectural Implication:** Ingested frequently to support energy production forecasting models (Use Case U2).

### 1.5 Intervention Reports & Thermal Imagery
- **Volume:** ~8,000 files (~15 GB binary storage).
- **Velocity:** Event-driven batch ingestion whenever technicians complete plant maintenance.
- **Variety:** Unstructured binary data including technician PDF reports, infrared JPEG photos, and accompanying JSON metadata.
- **Architectural Implication:** Demands an S3-compatible Object Storage solution capable of handling binary objects efficiently alongside partition metadata.

---

## 2. Comparative Analysis & Technology Selection

### 2.1 Storage System Comparison
| Criteria | Option A: SeaweedFS / MinIO (S3 API Compatible) | Option B: Apache HDFS | Selected Solution & Justification |
| :--- | :--- | :--- | :--- |
| **Resource Footprint** | Extremely lightweight, low memory overhead, native single-binary/container support. | Heavy Java daemon footprint (NameNode, DataNode, Secondary NameNode). | **SeaweedFS / MinIO (S3 API)** |
| **Deployment Complexity** | Fits easily into Docker Compose on a single VM. | Complex cluster configuration, unsuitable for single-host constraint. | Chosen for strict S3 API compatibility, high efficiency with small and large files, and low resource overhead on a single Docker host. |
| **Access Rights** | Standard S3 IAM policy model (buckets, prefixes, access keys). | POSIX-like ACLs. | |

### 2.2 Batch Processing Tool Comparison
| Criteria | Option A: Apache Airflow | Option B: Scheduled Python / Cron | Selected Solution & Justification |
| :--- | :--- | :--- | :--- |
| **Orchestration & Visibility** | Rich UI, DAG dependency management, built-in retry mechanisms. | Basic execution logs, no UI, manual retry logic. | **Apache Airflow** (or Python Scheduler depending on resource limits) |
| **Maintainability & Idempotency** | High maintainability, automatic backfilling, lineage tracing. | Low maintainability, hard to trace complex DAGs. | Chosen for robust workflow orchestration, error recovery, and clear monitoring for batch pipelines. |

### 2.3 Real-Time Streaming Tool Comparison
| Criteria | Option A: Redpanda / Apache Kafka | Option B: RabbitMQ | Selected Solution & Justification |
| :--- | :--- | :--- | :--- |
| **Message Model** | Distributed commit log, replayable streams, high write throughput. | Queue-based consumer pop model (non-replayable by default). | **Redpanda / Apache Kafka** |
| **Storage Persistence** | Native long-term log retention. | Transient messaging focus. | Chosen because stream replay is required for micro-batching into raw object storage. Redpanda provides full Kafka API compatibility with zero JVM overhead. |

---

## 3. Data Lake Layering & Zoning Strategy

To ensure data integrity, auditability, and clear separation of concerns, the storage container is structured into four functional zones:

1. **`raw/` (Landing Zone):** Immutable store receiving exact, unmodified original files (raw JSON streams, unmodified CSVs, PDFs, JPEGs). Serves as the single source of truth for replayability.
2. **`staging/` (Standardized Zone):** Standardized datasets. Timestamp formats unified to UTC ISO-8601, multi-vendor inverter JSON schemas aligned, and basic data quality validations enforced.
3. **`curated/` (Analytics & Business Zone):** Fully aggregated, schema-enforced datasets ready for Machine Learning (predictive maintenance, production forecasting) and reporting.
4. **`archive/` (Compliance & Cold Storage Zone):** Cold storage location for aged data governed by legal retention frameworks and GDPR cleanup policies.

---

## 4. Storage Naming & Partitioning Conventions

Standardized partition patterns are enforced across all zones to optimize query performance and ingestion logical paths:

- **Telemetry:** `raw/telemetry/vendor=<vendor_name>/year=<YYYY>/month=<MM>/day=<DD>/`
- **Smart Meter Curves:** `raw/load_curves/year=<YYYY>/month=<MM>/day=<DD>/`
- **Contracts:** `raw/contracts/year=<YYYY>/month=<MM>/`
- **Weather Data:** `raw/weather/site_id=<site>/year=<YYYY>/month=<MM>/day=<DD>/`
- **Intervention Reports:** `raw/interventions/site_id=<site>/year=<YYYY>/`
