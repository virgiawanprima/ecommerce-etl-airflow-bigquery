<div align="center">

# Enterprise E-Commerce Automated Data Engineering Pipeline
### Cloud-Native ELT Architecture, Kimball Star Schema, In-Flight Quality Gate, and SHA-256 PII Protection

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.0.6-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Google BigQuery](https://img.shields.io/badge/Google%20BigQuery-Data%20Warehouse-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-7%20Services-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Celery](https://img.shields.io/badge/Executor-Celery-37B34A?style=for-the-badge&logo=celery&logoColor=white)](https://docs.celeryq.dev/)
[![Looker Studio](https://img.shields.io/badge/BI-Looker%20Studio-EA4335?style=for-the-badge&logo=google&logoColor=white)](https://lookerstudio.google.com/)
[![CI](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/virgiawanprima/ecommerce-etl-airflow-bigquery/actions)

<p align="center">
  <b>Production-grade automated data pipeline processing 112,650 e-commerce transaction records into Google BigQuery using Ralph Kimball Star Schema, Apache Airflow 3 distributed orchestration, watermark incremental loading, cryptographic PII anonymization, and a 4-dimensional in-flight Quality Gate.</b>
</p>

<p align="center">
  <a href="#case-study-enterprise-data-engineering-pipeline"><b>English Case Study</b></a> •
  <a href="#studi-kasus-pipeline-data-engineering-enterprise"><b>Studi Kasus Bahasa Indonesia</b></a> •
  <a href="#system-architecture-and-end-to-end-data-flow"><b>Architecture</b></a> •
  <a href="#data-modeling-star-schema"><b>Data Model</b></a> •
  <a href="#verified-empirical-impact-and-performance-benchmarks"><b>Benchmarks</b></a> •
  <a href="#reproducible-environment-and-local-quickstart"><b>Quickstart</b></a> •
  <a href="#automated-verification-suite"><b>Verification</b></a> •
  <a href="#academic-research-context"><b>Academic Context</b></a>
</p>

</div>

---

## Case Study: Enterprise Data Engineering Pipeline

### 1. Business Context and Problem Statement

#### 1.1 Context and Operational Landscape
Modern e-commerce enterprises rely on timely, accurate, and compliant operational data to drive pricing decisions, logistics optimization, and executive reporting. This project addresses the transaction processing lifecycle of the Brazilian Olist e-commerce ecosystem, comprising 112,650 order items, 99,441 unique orders, and 3,095 sellers across 9 relational source tables spanning 2016 through 2018.

#### 1.2 Core Business and Technical Problems
Prior to engineering this automated pipeline, operational analytics suffered from three critical challenges:
1. **High Analytical Query Latency and Join Complexity:** Querying raw OLTP normalized schemas required 5-way to 8-way joins across gigabytes of operational tables, leading to query timeouts and high scanning costs in columnar data warehouses.
2. **Data Quality Degradation and Silent Ingestion Failures:** Real-world transaction streams frequently contain data anomalies such as missing customer references, duplicate composite primary keys, negative prices, and malformed strings. Unmonitored batch ingestion allows corrupted records to pollute analytical warehouses, distorting gross merchandise value (GMV) and executive KPIs.
3. **Regulatory Non-Compliance and PII Exposure:** Transaction logs exposed raw customer identities and plaintext identifiers, directly violating international data protection laws (such as GDPR and Indonesia UU No. 27 Tahun 2022 on Personal Data Protection / UU PDP).
4. **Pipeline Inefficiency and Redundant Processing:** Re-ingesting entire historical datasets on daily schedules caused unnecessary cloud processing overhead, network congestion, and extended batch execution windows.

---

### 2. Constraints and System Boundaries

1. **Zero-Budget Infrastructure and Sandbox Constraints:** The system was engineered to run seamlessly within the Google Cloud BigQuery Sandbox and containerized local infrastructure without incurring paid compute costs. This imposed strict quotas on API calls, storage policies, and query concurrency.
2. **BigQuery Sandbox 60-Day Partition Expiration Rule:** Empirical testing revealed that BigQuery Sandbox automatically assigns a 60-day default expiration to partitioned tables. Because the historical Olist dataset dates from 2016 to 2018, partition-by-date policies in Sandbox caused historical partitions to be dropped immediately after ingestion. The production design had to account for this without compromising data durability.
3. **Mandatory Cryptographic Pseudonymization:** All customer identification keys (`customer_unique_id`) were required to be irrecoverably hashed using SHA-256 before landing in staging or production analytical tables.
4. **Strict Fail-Fast Quality Threshold (<5%):** Business requirements mandated that if composite anomaly rates in any batch exceed 5%, the pipeline must immediately abort execution before invoking the `load` step, isolating malformed records into an audit dead-letter log (`fact_sales_error_log.csv`).

---

### 3. Architectural Decisions and Technical Trade-Offs

#### 3.1 Kimball Star Schema vs OLTP Denormalization
- **Choice:** Dimensional modeling based on Ralph Kimball Star Schema, featuring 1 centralized fact table (`fact_sales`) and 5 conformed dimension tables (`dim_customers`, `dim_products`, `dim_sellers`, `dim_orders`, `dim_date`).
- **Trade-off:** Denormalizing into a single wide flat table would eliminate joins entirely but create massive row redundancy across 32,951 products and 99,441 customers, increasing storage costs and ETL maintenance overhead. The Star Schema reduces query join depth from 9 tables to single-hop joins while preserving clean analytical granularity.

#### 3.2 In-Memory PyArrow and Pandas vs Apache Spark
- **Choice:** Python 3.12 with PyArrow and vectorized Pandas.
- **Trade-off:** For batch volumes of 112,650 records (approximately 100 MB raw CSV data), running a distributed Apache Spark cluster introduces substantial JVM initialization latency, worker serialization overhead, and cluster infrastructure costs. In-memory PyArrow processing completes full transformation in approximately 3 seconds on a single core with less than 500 MB RAM consumption. Spark remains the designated scaling path once batch volumes exceed 50 million rows.

#### 3.3 Watermark-Driven Incremental Ingestion vs Full Table Refresh
- **Choice:** High-watermark incremental ingestion querying BigQuery `MAX(order_date)`.
- **Trade-off:** Full table refreshes (`WRITE_TRUNCATE`) guarantee idempotency but waste bandwidth and BigQuery storage write API operations as data grows. The watermark strategy selectively appends new delta transactions (`WRITE_APPEND`) while refreshing conformed dimensions, reducing delta payload transmission by over 99% during steady-state runs.

#### 3.4 In-DAG Quality Gate vs Post-Ingestion dbt Testing
- **Choice:** In-DAG Quality Dimensions Check (QDC) engine placed strictly before the BigQuery `load` step.
- **Trade-off:** Post-load validation tools (such as dbt test or Great Expectations running after warehouse insertion) permit bad records to enter production tables, requiring costly rollbacks or staging cleanup. Embedding validation directly in-flight guarantees zero corrupted records enter the warehouse. The runtime overhead is only +7.9% (+4.55 seconds).

#### 3.5 Distributed CeleryExecutor vs LocalExecutor
- **Choice:** Apache Airflow 3.0.6 orchestrated via CeleryExecutor backed by Redis message queues and PostgreSQL metadata backend across 7 Docker Compose containers.
- **Trade-off:** CeleryExecutor requires additional operational services (Redis broker, Celery worker) compared to LocalExecutor, but it decouples task execution from the webserver and scheduler, supporting independent worker scaling and crash isolation.

---

### 4. Measurable Verified Impact

Every outcome below is supported by reproducible empirical test logs documented in this repository:

| Engineering Dimension | Target / SLA Threshold | Actual Measured Result | Status |
| :--- | :--- | :--- | :--- |
| **Batch Processing Velocity** | Less than 15 minutes (900 seconds) | **39 to 57 seconds** (Full Load) | Passed |
| **Data Quality Gate Pass Rate** | Greater than or equal to 95.0% | **100.0% Pass** on clean data (0 unhandled anomalies) | Passed |
| **Synthetic Noise Detection Precision** | 100% anomaly detection | **100.0% precision** across 2,252 (2%) and 9,012 (8%) noisy rows | Passed |
| **Fail-Fast Trigger Enforcement** | Abort `load` if noise is greater than 5% | **Verified**: Load skipped (`try_number=0`), 0 bad rows in warehouse | Passed |
| **PII Data Privacy Compliance** | Zero plaintext identifiers | **0 plaintext customer IDs**, 100% 64-char SHA-256 entropy | Passed |
| **Scheduling Regularity** | Deviation less than 5.0% | **+0.6s average deviation** (+1.0s max, 0.56% of interval) | Passed |
| **Pipeline Reliability (Audit)** | Historical failure rate less than 10% | **4.17% historical failure rate** over 48 DAG runs (46 successes) | Passed |

---

## System Architecture and End-to-End Data Flow

```
┌─────────────────────────┐       ┌───────────────────────────────┐       ┌─────────────────────────┐       ┌────────────────────────┐
│     9 Raw CSV Files     │  ───> │   Apache Airflow 3.0.6        │  ───> │  Google BigQuery        │  ───> │  Looker Studio         │
│     data/raw/           │       │   CeleryExecutor (7 Services) │       │  Dataset: olist_dw      │       │  Executive BI          │
└─────────────────────────┘       └───────────────────────────────┘       └─────────────────────────┘       └────────────────────────┘
    99,441 Orders                      5-Task Modular Pipeline                 6 Star Schema Tables                Live BigQuery Query
    112,650 Order Items                Execution Duration: ~60s                Region: asia-southeast2             Interactive Dashboards
```

### Infrastructure and Service Composition

| Service Name | Component and Version | Functional Responsibility |
| :--- | :--- | :--- |
| `airflow-webserver` | Apache Airflow 3.0.6 UI | Web user interface, authentication, DAG inspection, and task monitoring |
| `airflow-scheduler` | Airflow Scheduler Engine | DAG parsing, dependency evaluation, cron scheduling, and state coordination |
| `airflow-worker` | Celery Worker (Python 3.12) | Distributed execution of data extraction, validation, transformation, and load |
| `airflow-triggerer` | Airflow Triggerer Engine | Asynchronous deferred event handling and sensor monitoring |
| `postgres` | PostgreSQL 16 Alpine | Relational metadata store tracking DAG runs, task instances, and XCom payloads |
| `redis` | Redis 7.2 Alpine | In-memory message broker queuing tasks for distributed Celery workers |
| `bigquery` | Google Cloud BigQuery | Cloud-native serverless analytical columnar data warehouse |

---

## Data Modeling: Star Schema

The analytical warehouse implements a Ralph Kimball Star Schema centered on transactional sales events.

```mermaid
erDiagram
    dim_customers ||--o{ fact_sales : "customer_id"
    dim_products ||--o{ fact_sales : "product_id"
    dim_sellers ||--o{ fact_sales : "seller_id"
    dim_orders ||--o{ fact_sales : "order_id"
    dim_date ||--o{ fact_sales : "date_key"

    fact_sales {
        STRING order_id
        INT order_item_id
        STRING customer_id FK
        STRING product_id FK
        STRING seller_id FK
        DATE order_date
        INT date_key FK
        FLOAT price
        FLOAT freight_value
    }

    dim_customers {
        STRING customer_id PK
        STRING customer_unique_id
        STRING customer_city
        STRING customer_state
    }

    dim_products {
        STRING product_id PK
        STRING product_category_name
        STRING product_category_name_english
    }

    dim_sellers {
        STRING seller_id PK
        STRING seller_city
        STRING seller_state
    }

    dim_orders {
        STRING order_id PK
        STRING customer_id FK
        STRING order_status
        DATETIME order_purchase_timestamp
    }

    dim_date {
        INT date_key PK
        DATE date
        INT year
        INT month
        INT day
        INT quarter
        INT day_of_week
        BOOL is_weekend
    }
```

### Table Specifications and Granularity

| Table Name | Role | Record Count | Primary Key / Composite Key | Grain Description |
| :--- | :--- | :---: | :--- | :--- |
| `fact_sales` | Fact | 112,650 | `order_id` + `order_item_id` | Individual product item within a completed customer order |
| `dim_customers` | Dimension | 99,441 | `customer_id` | Customer geographical location and hashed identity |
| `dim_products` | Dimension | 32,951 | `product_id` | Product catalog categories (Portuguese and English) |
| `dim_sellers` | Dimension | 3,095 | `seller_id` | Merchant geographical location (city, state) |
| `dim_orders` | Dimension | 99,441 | `order_id` | Order status and lifecycle purchase timestamp |
| `dim_date` | Dimension | 634 | `date_key` (YYYYMMDD) | Calendar attributes, quarter, day of week, weekend flags |

---

## Pipeline Workflow and Task Execution

The DAG `olist_etl_pipeline` executes 5 idempotent, modular tasks:

```
[ extract ] ──> [ validate_schema ] ──> [ transform ] ──> [ qdc_validation ] ──> [ load ]
```

1. **Extract (`extract`):** Reads 9 raw CSV files from `data/raw/` via vectorized PyArrow. Concurrently queries the destination BigQuery warehouse for `MAX(order_date)` to dynamically decide between Full Load (`WRITE_TRUNCATE`) and Incremental Delta Load (`WRITE_APPEND`).
2. **Validate Schema (`validate_schema`):** Enforces schema contracts across all tables. Detects missing mandatory columns or unexpected structural mutations with immediate fail-fast termination before memory-heavy operations begin.
3. **Transform (`transform`):** Cleans dates, standardizes datatypes, constructs the Kimball Star Schema joins, filters delta rows when watermark is present, and hashes `customer_unique_id` using SHA-256 for privacy compliance.
4. **QDC Validation (`qdc_validation`):** Evaluates data across 4 dimensions:
   - *Completeness:* Enforces NOT NULL constraints on mandatory keys and financial metrics.
   - *Uniqueness:* Enforces composite primary key uniqueness (`order_id` + `order_item_id`).
   - *Consistency:* Validates foreign key integrity across fact and dimension tables.
   - *Validity:* Enforces domain rules (`price >= 0`, `freight_value >= 0`, valid calendar dates).
   If anomaly ratio is greater than 5%, execution halts with `ValueError`, and bad records are routed to `data/processed/fact_sales_error_log.csv`.
5. **Load (`load`):** Uploads validated Parquet files to Google BigQuery. Incorporates automated exponential backoff retry policies (3 attempts with 5-minute delays).

---

## Verified Empirical Impact and Performance Benchmarks

### Full Load Execution Profile (112,650 records)

| Pipeline Task | Average Duration | Percentage | Execution Responsibilities |
| :--- | :---: | :---: | :--- |
| `extract` | 5.0 seconds | 8.3% | Read 9 CSV sources and query BigQuery watermark |
| `validate_schema` | 1.5 seconds | 2.5% | Verify schema contracts and column presence across 9 tables |
| `transform` | 2.5 seconds | 4.2% | Join Star Schema, clean datatypes, and apply SHA-256 PII hashing |
| `qdc_validation` | 3.0 seconds | 5.0% | Test 4 quality dimensions, evaluate 5% fail-fast threshold |
| `load` | 45.0 seconds | 75.0% | Stream Parquet tables to BigQuery storage APIs in parallel |
| **Total Pipeline** | **~57.0 seconds** | **100.0%** | **Completed well under 900-second SLA limit** |

### Synthetic Noise Injection and Fail-Fast Test Results

To empirically prove data quality gate behavior, controlled synthetic noise was injected across all 4 quality dimensions:

| Test Scenario | Total Injected Noise | Detected Anomalies | Detection Accuracy | Dead-Letter Log | Fail-Fast Triggered | BigQuery Ingestion |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Clean Baseline** | 0 rows | 0 rows | 100.0% | 0 rows | No | 112,650 rows |
| **2% Controlled Noise** | 2,252 rows | 2,252 rows | 100.0% | 2,252 rows written | No (under 5% limit) | 110,398 rows loaded |
| **8% Severe Noise** | 9,012 rows | 9,012 rows | 100.0% | 9,012 rows written | **Yes** (`ValueError`) | **Halted (0 rows loaded)** |

In the 8% scenario, the Airflow scheduler recorded `try_number=0` for the `load` task, proving zero dirty records reached the data warehouse.

### Quality Dimensions Check Overhead Analysis

| Pipeline Mode | Wall Clock Duration | Task Execution Sum | Computational Overhead |
| :--- | :---: | :---: | :---: |
| `olist_etl_baseline_noqdc` (without quality gate) | 57.56 seconds | 55.05 seconds | Reference baseline |
| `olist_etl_pipeline` (with active 4-dimension QDC) | 62.11 seconds | 58.74 seconds | +4.55 seconds |
| **Net Quality Engine Cost** | **+4.55 seconds** | **+3.69 seconds** | **+7.9% runtime overhead** |

---

## Reproducible Environment and Local Quickstart

### Prerequisites
- Docker Engine 24+ and Docker Compose v2+ (WSL2 supported on Windows).
- Google Cloud Platform account with BigQuery Admin / Data Editor permissions.
- Olist dataset 9 raw CSV files placed in `data/raw/`.

### Installation Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/virgiawanprima/ecommerce-etl-airflow-bigquery.git
   cd ecommerce-etl-airflow-bigquery
   ```

2. **Configure environment variables:**
   ```bash
   cp .env.example .env
   ```
   Set your `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, and local path variables inside `.env`.

3. **Install GCP service account credentials:**
   Save your Google Cloud Service Account key to:
   ```
   config/keys/gcp-sa-key.json
   ```

4. **Launch Airflow infrastructure:**
   ```bash
   docker compose up -d
   ```

5. **Trigger pipeline execution:**
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger olist_etl_pipeline
   ```

6. **Access Airflow web interface:**
   Navigate to `http://localhost:8080` (Default credentials: `airflow` / `airflow`).

---

## Automated Verification Suite

The repository contains an automated verification suite via `Makefile` requiring zero external test dependencies:

```bash
make verify
```

| Makefile Target | Test Scope and Verification Coverage |
| :--- | :--- |
| `make dag-check` | Verifies Python syntax, 5-task DAG registration, and pip dependency tree integrity |
| `make self-check` | Executes 19 assert-based unit tests for QDC logic, fail-fast boundary, and SHA-256 hashing |
| `make bq-verify` | Directly queries BigQuery to confirm actual live row counts for all 6 Star Schema tables |
| `make run` | Triggers an idempotent execution of the main `olist_etl_pipeline` DAG |
| `make run-baseline` | Triggers the baseline pipeline without QDC to measure runtime differences |

### BigQuery Verification Query

Execute the following query in the Google BigQuery Console to verify table population:

```sql
SELECT 
    COUNT(*) AS total_sales_records,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(price), 2) AS total_gross_revenue_brl
FROM `your-gcp-project.olist_dw.fact_sales`;
-- Expected Target: 112,650 rows | 98,666 unique orders | R$ 13,591,248.91 gross revenue
```

---

<br>

---

## Studi Kasus: Pipeline Data Engineering Enterprise

### 1. Konteks dan Masalah Bisnis

#### 1.1 Konteks dan Lanskap Operasional
Organisasi e-commerce modern bergantung pada ketersediaan data transaksi yang cepat, akurat, dan sesuai regulasi untuk mendukung penetapan harga dinamis, optimasi logistik, serta pelaporan eksekutif. Studi kasus ini menggunakan ekosistem data e-commerce Olist Brazil yang memuat 112.650 baris item pesanan, 99.441 pesanan unik, dan 3.095 penjual dari 9 tabel relasional publik rentang tahun 2016 sampai 2018.

#### 1.2 Masalah Utama Bisnis dan Teknis
Sebelum pipeline terotomatisasi ini dirancang, operasional analitik menghadapi kendala signifikan:
1. **Latensi Kueri Tinggi akibat Kompleksitas Join OLTP:** Analisis bisnis terhadap skema normalisasi transaksional membutuhkan operasi join 5 hingga 8 tabel relasional berukuran besar, memicu timeout kueri dan membengkaknya biaya pemindaian komputasi (*query scan cost*) pada data warehouse kolumnar.
2. **Kerusakan Kualitas Data dan Ingestion Gagal Senyap:** Aliran transaksi riil kerap mengandung anomali data seperti hilangnya referensi pelanggan, duplikasi kunci komposit, nilai harga negatif, dan anomali tipe data. Pemuatan data tanpa validasi gerbang menyebabkan data rusak masuk ke data warehouse dan mendistorsi metrik pendapatan (*Gross Merchandise Value*).
3. **Ketidakpatuhan Regulasi Privasi Data Pribadi:** Data transaksi mentah mengekspos identitas pelanggan langsung dalam bentuk teks terbaca (*plaintext*), melanggar regulasi privasi data global seperti GDPR dan regulasi nasional UU No. 27 Tahun 2022 tentang Pelindungan Data Pribadi (UU PDP).
4. **Inefisiensi Batch ETL Berulang:** Pemrosesan ulang seluruh data historis setiap hari membebani jaringan dan memperpanjang jendela eksekusi batch tanpa memberikan nilai tambah.

---

### 2. Hambatan dan Batasan Sistem

1. **Batasan Anggaran Komputasi Nol (Free-Tier Sandbox):** Sistem dirancang agar dapat beroperasi penuh pada tingkat gratis Google BigQuery Sandbox dan infrastruktur kontainer lokal tanpa menimbulkan biaya komputasi berbayar.
2. **Kebijakan Kedaluwarsa Partisi 60 Hari pada BigQuery Sandbox:** Pengujian empiris membuktikan bahwa BigQuery Sandbox menetapkan masa aktif partisi maksimal 60 hari. Karena data historis Olist berasal dari 2016 sampai 2018, partisi tabel otomatis terhapus sesaat setelah dimuat. Arsitektur sistem harus mengadopsi tabel non-partitioned agar seluruh data historis tersimpan persisten secara utuh.
3. **Kewajiban Pseudonimisasi Kriptografis PII:** Seluruh identitas unik pelanggan (`customer_unique_id`) wajib dienkripsi satu arah menggunakan algoritma SHA-256 sebelum dimuat ke tabel pementasan maupun gudang data.
4. **Kebijakan Ambang Batas Fail-Fast Ketat (<5%):** Jika rasio anomali data dalam satu batch melebihi 5%, pipeline wajib menghentikan eksekusi secara otomatis sebelum tahap pemuatan (*load*), serta mengalihkan data cacat ke berkas audit dead-letter (`fact_sales_error_log.csv`).

---

### 3. Keputusan Arsitektur dan Pertimbangan Teknis

#### 3.1 Star Schema Ralph Kimball Dibandingkan Denormalisasi Tabel Tunggal
- **Keputusan:** Mengadopsi pemodelan dimensional Star Schema Ralph Kimball yang terdiri dari 1 tabel fakta transaksi (`fact_sales`) dan 5 tabel dimensi (`dim_customers`, `dim_products`, `dim_sellers`, `dim_orders`, `dim_date`).
- **Analisis Trade-Off:** Denormalisasi tabel tunggal yang sangat lebar (*wide flat table*) memang meniadakan operasi join, namun menciptakan redundansi data ekstrem untuk 32.951 produk dan 99.441 pelanggan. Star Schema membatasi join menjadi satu langkah relasi langsung dari fakta ke dimensi, mempercepat kueri analitik Looker Studio menjadi di bawah satu detik sekaligus menjaga efisiensi penyimpanan.

#### 3.2 Pemrosesan PyArrow dan Pandas In-Memory Dibandingkan Apache Spark
- **Keputusan:** Memanfaatkan Python 3.12 dengan pustaka tervektorisasi PyArrow dan Pandas.
- **Analisis Trade-Off:** Untuk volume 112.650 baris data (sekitar 100 MB data mentah), klaster terdistribusi Apache Spark membutuhkan overhead inisialisasi JVM dan serialisasi antar-worker selama beberapa menit. PyArrow in-memory menyelesaikan seluruh transformasi dalam waktu 3 detik pada satu inti CPU dengan memori di bawah 500 MB.

#### 3.3 Muat Inkremental Berbasis Watermark Dibandingkan Muat Penuh
- **Keputusan:** Menggunakan penanda batas air (*watermark*) `MAX(order_date)` langsung dari BigQuery.
- **Analisis Trade-Off:** Muat ulang penuh (*WRITE_TRUNCATE*) menjamin konsistensi tetapi memboroskan kuota penulisan API BigQuery. Mekanisme watermark secara otomatis memisahkan transaksi baru untuk ditambahkan (*WRITE_APPEND*) sambil memperbarui dimensi yang sesuai, menghemat transfer data lebih dari 99% pada kondisi operasional stabil.

#### 3.4 Gerbang Kualitas In-DAG Dibandingkan Audit Pasca-Muat
- **Keputusan:** Menerapkan mesin Quality Dimensions Check (QDC) terintegrasi langsung di dalam DAG sebelum task `load`.
- **Analisis Trade-Off:** Pengecekan kualitas setelah data masuk gudang data (misalnya pengujian dbt) memungkinkan data cacat sempat terbaca oleh dashboard bisnis. Menempatkan gerbang kualitas sebelum muat menjamin hanya data bersih yang masuk ke BigQuery, dengan biaya overhead komputasi yang sangat rendah (hanya +7,9% atau +4,55 detik).

#### 3.5 CeleryExecutor Terdistribusi pada Docker Compose
- **Keputusan:** Menggunakan Apache Airflow 3.0.6 dengan CeleryExecutor, antrean pesan Redis, dan metadata PostgreSQL pada 7 kontainer Docker.
- **Analisis Trade-Off:** Memerlukan kontainer pendukung tambahan dibandingkan LocalExecutor, namun memberikan pemisahan proses yang kokoh antara penjadwal, peramban web, dan worker eksekusi sehingga mencegah kegagalan sistem terpusat.

---

### 4. Dampak Terukur dan Bukti Pengujian Empiris

Setiap klaim keberhasilan pada tabel berikut didukung oleh bukti pengujian empiris yang dapat diverifikasi pada berkas dokumentasi pengujian repositori ini:

| Dimensi Pengujian | Target / Ambang Batas SLA | Hasil Aktual Pengujian | Status Evaluasi |
| :--- | :--- | :--- | :--- |
| **Kecepatan Pemrosesan Data** | Kurang dari 15 menit (900 detik) | **39 sampai 57 detik** (Muat Penuh) | Lulus |
| **Tingkat Kelulusan Kualitas Data** | Lebih dari atau sama dengan 95,0% | **100,0% Lulus** pada data bersih (0 anomali) | Lulus |
| **Presisi Deteksi Noise Sintetis** | Deteksi anomali 100% tepat | **100,0% presisi** pada 2.252 (2%) dan 9.012 (8%) baris uji | Lulus |
| **Penerapan Batas Fail-Fast** | Hentikan load jika anomali melebihi 5% | **Terbukti**: Task load diabaikan (`try_number=0`), 0 baris masuk | Lulus |
| **Kepatuhan Privasi Data PII** | Tidak ada identitas teks terbaca | **0 baris teks terbaca**, 100% hash SHA-256 64 karakter | Lulus |
| **Akurasi Jadwal Eksekusi** | Deviasi jadwal di bawah 5,0% | **+0,6 detik deviasi rata-rata** (+1,0 detik maks, 0,56% interval) | Lulus |
| **Keandalan Riwayat Pipeline** | Tingkat kegagalan di bawah 10% | **4,17% tingkat kegagalan riil** dari 48 run (46 berhasil) | Lulus |

---

### 5. Arsitektur Sistem dan Rincian Alur Pemrosesan

Alur kerja DAG `olist_etl_pipeline` dibagi menjadi 5 task berurutan yang bersifat idempoten:

1. **Extract (`extract`):** Membaca 9 file CSV mentah menggunakan PyArrow serta mengambil watermark `MAX(order_date)` dari BigQuery untuk menentukan mode Full Load atau Incremental.
2. **Validate Schema (`validate_schema`):** Memverifikasi integritas struktur kolom input pada 9 tabel untuk memastikan kontrak data terpenuhi sejak awal.
3. **Transform (`transform`):** Menstandarisasi format data, merangkai relasi Star Schema, memfilter transaksi baru berdasarkan watermark, dan menerapkan hashing SHA-256 pada data pelanggan sensitif.
4. **QDC Validation (`qdc_validation`):** Menguji data transaksi berdasarkan 4 dimensi kualitas:
   - *Completeness:* Memastikan tidak ada nilai kosong pada kolom kunci dan metrik harga.
   - *Uniqueness:* Memastikan tidak ada duplikasi kunci komposit (`order_id` + `order_item_id`).
   - *Consistency:* Memvalidasi integritas referensial antara tabel fakta dan tabel dimensi.
   - *Validity:* Memastikan batasan logika domain terpenuhi (`price >= 0`, `freight_value >= 0`, rentang tanggal valid).
   Jika rasio anomali melebihi 5%, sistem memicu penghentian fail-fast dan mencatat data cacat ke `data/processed/fact_sales_error_log.csv`.
5. **Load (`load`):** Mengunggah data tabel yang telah tervalidasi ke Google BigQuery dengan kebijakan percobaan ulang otomatis (*retry*) sebanyak 3 kali dan jeda 5 menit.

---

### 6. Bukti Pengujian Injeksi Anomali dan Fail-Fast

Pengujian injeksi noise sintetis dilakukan untuk menguji keandalan gerbang kualitas:

| Skenario Pengujian | Jumlah Noise | Terdeteksi | Presisi Deteksi | Berkas Catatan Error | Status Fail-Fast | Pemuatan ke BigQuery |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Data Bersih** | 0 baris | 0 baris | 100,0% | 0 baris | Tidak aktif | 112.650 baris termuat |
| **Injeksi Noise 2%** | 2.252 baris | 2.252 baris | 100,0% | 2.252 baris tersimpan | Tidak aktif (<5%) | 110.398 baris termuat |
| **Injeksi Noise 8%** | 9.012 baris | 9.012 baris | 100,0% | 9.012 baris tersimpan | **Aktif** (`ValueError`) | **Dibatalkan (0 baris termuat)** |

Pada pengujian 8%, task `load` mencatat `try_number=0` (tidak pernah dieksekusi), membuktikan bahwa gerbang fail-fast berhasil melindungi data warehouse dari data kotor.

---

## Repository Structure

```text
olist-etl-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                # CI automation: Python linting, DAG integrity, unit test suite
├── .dockerignore                 # Excludes secrets, local logs, and raw datasets from build context
├── .env.example                  # Secure template for environment variables and GCP configuration
├── .gitignore                    # Version control exclusion rules for caches, keys, and data
├── Dockerfile                    # Custom Apache Airflow 3.0.6 image with Google Cloud dependencies
├── Makefile                      # Standardized test and execution entry points (make verify)
├── README.md                     # Comprehensive technical documentation and case study report
├── docker-compose.yml            # 7 Airflow microservices (CeleryExecutor, Redis, PostgreSQL)
├── requirements.txt              # Pinned Python dependencies (pandas, pyarrow, google-cloud-bigquery)
├── config/
│   └── keys/
│       └── gcp-sa-key.json       # GCP Service Account key credentials (gitignored for security)
├── dags/
│   ├── olist_etl_baseline_noqdc.py # Benchmark baseline DAG without quality gate
│   └── olist_etl_pipeline.py     # Main 5-task production pipeline with integrated QDC gate
├── src/
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── io_helpers.py         # Vectorized I/O utilities and Parquet read/write routines
│   │   ├── pii.py                # SHA-256 cryptographic hashing protection for personal data
│   │   ├── qdc.py                # 4-dimensional Quality Dimensions Check validation engine
│   │   └── self_check.py         # 19-assertion unit test suite verifying logic boundaries
│   └── scripts/
│       ├── __init__.py
│       └── noise_injection.py    # Synthetic anomaly injector validating fail-fast gate behavior
├── data/                         # Local storage volumes (gitignored)
│   ├── _noise_backup/            # Checkpoint backup for synthetic noise test runs
│   ├── processed/                # Local staging directory for Parquet files and error logs
│   └── raw/                      # 9 raw CSV dataset files from the Brazilian Olist ecosystem
├── docs/                         # Detailed performance and empirical testing records
│   ├── bab4_hasil_pengujian.md   # Comprehensive empirical testing reports and query outputs
│   ├── jawaban_evaluasi_claude.md# Evaluation response log and Looker Studio reconciliation notes
│   └── metrik_performa.md        # Consolidated SLA benchmarks and runtime duration logs
└── logs/                         # Container execution logs (gitignored)
```

---

## Organized Change History: Conventional Commits

Project development strictly follows the [Conventional Commits](https://www.conventionalcommits.org/) standard to guarantee a transparent, audit-ready version history:

- `feat`: Addition of new pipeline stages, transformation logic, or quality check features.
- `fix`: Bug resolutions such as DAG registration scoping, object datatype coercion, or join cardinality fixes.
- `refactor`: Structural codebase improvements without modifying functional execution outputs.
- `docs`: Additions or refinements to technical documentation, performance logs, and README case studies.
- `ci`: Updates to GitHub Actions workflows, test configurations, or container build matrices.
- `chore`: Maintenance tasks including gitignore configurations and dependency bumps.

---

## System Limitations and Future Engineering Roadmap

1. **BigQuery Sandbox Partitioning Boundary:** In the current sandbox environment, table partitioning is disabled to avoid the automatic 60-day deletion of historical data (2016-2018). In an enterprise production deployment with active Google Cloud billing, tables should be migrated to daily ingestion-time partitioning (`PARTITION BY order_date`) paired with clustering on `customer_state` and `product_id` to optimize kueri scan efficiency on multi-terabyte tables.
2. **Horizontal Worker Autoscaling:** The current CeleryExecutor deployment runs on a single container host. Scaling to enterprise streaming velocities can be achieved by transitioning worker orchestration to Kubernetes via the Apache Airflow KubernetesExecutor or CeleryKubernetesExecutor.
3. **Data Contract Management:** Future enhancements can integrate schema contract definitions utilizing Great Expectations or dbt contract enforcement to detect upstream source format shifts automatically.

---

## Academic Research Context

This engineering project was originally conceived and validated as the technical implementation for an undergraduate engineering thesis:
- **Project Title:** Implementasi Pipeline ETL dan Dashboard Analitik Data Transaksi E-Commerce menggunakan Apache Airflow dan Google BigQuery
- **Institution:** Politeknik Negeri Samarinda, 2026
- **Specialization:** Data Engineering & Analytics Engineering

---

<div align="center">
  <sub>Engineered with rigorous adherence to clean code, verifiable technical impact, and enterprise data reliability standards.</sub>
</div>
