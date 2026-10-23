# Project Report: Electric Vehicle Charging Station Analytics using Apache Spark and Cassandra

**Course / Lab:** Big Data Analysis (BDA) Lab Project  
**Repository Location:** `E:\BDA ev charge`  
**Target Environment:** Python 3.11, Apache Spark 3.5.1, Apache Cassandra 4.1 (Docker), Streamlit, Plotly  

---

## 1. Project Title & Objective

**Project Title:** Electric Vehicle Charging Station Analytics using Apache Spark and Cassandra  
**Primary Objective:** Build an end-to-end Big Data analytics pipeline that ingests a real-world dataset of 72,856 EV charging transaction records, cleans and aggregates the data using distributed Apache Spark (PySpark), persists query-optimized analytical summaries into an Apache Cassandra NoSQL database, and presents actionable insights via an interactive Streamlit and Plotly dashboard.

---

## 2. Problem Statement & Motivation

Electric vehicles (EVs) are adopting rapidly worldwide, placing concentrated demands on municipal electrical distribution grids. Without understanding charging session characteristics—such as peak start-time windows, charging duration distributions, and differences between AC Level-2 and DC Fast Charging—grid operators face transformer overloads, while station operators struggle with inefficient charger deployment.

This project addresses these challenges by:
1. Processing raw M2M (machine-to-machine) commercial transaction telemetry at scale using distributed computing.
2. Modeling low-latency, query-driven NoSQL database tables in Apache Cassandra to eliminate expensive table joins and full-table scans.
3. Providing utility planners and station operators with interactive visual analytics on hourly demand profiles, station turnover rates, and facility utilization.

---

## 3. Real Dataset Specification

- **Title:** *A dataset for multi-faceted analysis of electric vehicle charging transactions*
- **Peer-Reviewed Reference:** *Nature Scientific Data* (2024), DOI: [10.1038/s41597-024-02942-9](https://doi.org/10.1038/s41597-024-02942-9)
- **Portal Attribution:** IEEE Task Force on Data Sharing in Energy Systems ([Dataset Reference](https://ieee-pes-data-sharing.org/datasets/detail/c903145b-3ba7-4fd3-ac1b-7727abf79e73))
- **Repository:** Figshare (DOI: `10.6084/m9.figshare.22495141`)
- **License:** Creative Commons Attribution 4.0 International (CC BY 4.0)
- **Local File:** `data/raw/ChargingRecords.csv` (Size: 7,605,859 bytes, ~7.25 MB)
- **Raw Dimensions:** Exactly **72,856 data rows** and **13 columns**

### Schema & Data Dictionary

| Column Name | Raw Type | PySpark Type | Units & Semantic Meaning |
| :--- | :--- | :--- | :--- |
| `UserID` | Integer | `IntegerType` | Anonymized unique identifier for the EV driver. |
| `ChargerID` | Integer | `IntegerType` | Physical charging station unit identifier. |
| `ChargerCompany` | Integer | `IntegerType` | Identifier for the charging station network operator. |
| `Location` | String | `StringType` | Facility classification (e.g., `public area`, `apartment`, `resort`, `hotel`). |
| `ChargerType` | Integer | `IntegerType` | Current type: `0` = AC (Slow / Level 2), `1` = DC (Fast / Rapid). |
| `StartDay` | String | `StringType` | Session commencement date (`YYYY-MM-DD`). |
| `StartTime` | String | `StringType` | Session commencement time (`HH:mm:ss`). |
| `EndDay` | String | `StringType` | Session completion date (`YYYY-MM-DD`). |
| `EndTime` | String | `StringType` | Session completion time (`HH:mm:ss`). |
| `StartDatetime` | String | `StringType` | Commencement timestamp without seconds (`YYYY-MM-DD HH:mm`). |
| `EndDatetime` | String | `StringType` | Completion timestamp without seconds (`YYYY-MM-DD HH:mm`). |
| `Duration` | Double | `DoubleType` | **Active session duration in minutes** (mean: 151.2 min). |
| `Demand` | Double | `DoubleType` | **Delivered electrical energy in kilowatt-hours (kWh)** (mean: 17.44 kWh). |

---

## 4. Technology Stack & Roles

```text
+---------------------------------------------------------------------------------+
|                                TECHNOLOGY STACK                                 |
+---------------------------------------------------------------------------------+
| Layer              | Technology            | Purpose / Role in Project          |
|--------------------+-----------------------+------------------------------------|
| Ingestion & Lake   | Local Filesystem      | Raw CSV & Cleaned Parquet storage  |
| Compute Engine     | Apache Spark 3.5.1    | Distributed DataFrame ETL pipeline |
| Serving Database   | Apache Cassandra 4.1  | Query-driven NoSQL analytical sink |
| Infrastructure     | Docker & Compose v2   | Single-node Cassandra container    |
| Presentation Tier  | Streamlit 1.38.0      | Web application framework          |
| Visualizations     | Plotly 5.24.1         | Interactive client-side charting   |
| Language & Testing | Python 3.11 & PyTest  | Typed business logic & test suites |
+---------------------------------------------------------------------------------+
```

---

## 5. System Architecture & Data Flow

```mermaid
flowchart TD
    subgraph DataIngestion["1. Raw Ingestion Layer"]
        RAW["data/raw/ChargingRecords.csv<br/>(72,856 Raw Transactions)"]
    end

    subgraph SparkProcessing["2. Distributed PySpark ETL Engine"]
        SCHEMA["Explicit StructType Schema"]
        DEDUP["Deduplication: dropDuplicates()<br/>(13 exact duplicates dropped)"]
        PARSE["Timestamp Parsing: to_timestamp()<br/>(StartDay + StartTime -> start_timestamp)"]
        FILTER["Validation Filter: Duration > 0 & Demand > 0<br/>(735 invalid records dropped)"]
        CLEAN_PARQUET["data/processed/cleaned_charging_records.parquet<br/>(72,108 Clean Records)"]
        AGG["Core Aggregations:<br/>- Overall KPIs<br/>- Hourly Demand (24h)<br/>- AC vs DC Metrics<br/>- Top 10 Locations"]
    end

    subgraph StorageSink["3. Data Persistence"]
        CSV_OUT["data/processed/analytics/*.csv"]
        CASSANDRA[("Apache Cassandra 4.1<br/>Port 9042 | Keyspace: ev_analytics<br/>- overall_metrics<br/>- hourly_demand<br/>- charger_type_metrics<br/>- location_metrics")]
    end

    subgraph Presentation["4. Presentation Tier"]
        DASH["Streamlit Dashboard (Port 8501)<br/>- 4 KPI Metric Cards<br/>- 24h Hourly Demand Profile<br/>- AC vs DC Duration Ratio<br/>- Siting Category Ranking<br/>- Dynamic Analytical Findings"]
    end

    RAW --> SCHEMA --> DEDUP --> PARSE --> FILTER
    FILTER --> CLEAN_PARQUET
    FILTER --> AGG
    AGG --> CSV_OUT
    CSV_OUT -->|load_cassandra.py| CASSANDRA
    CASSANDRA -->|CQL SELECT (Cached 60s TTL)| DASH
```

---

## 6. PySpark ETL Pipeline Implementation

The pipeline is implemented across [`src/analytics/spark_session.py`](file:///e:/BDA%20ev%20charge/src/analytics/spark_session.py), [`src/analytics/etl_pipeline.py`](file:///e:/BDA%20ev%20charge/src/analytics/etl_pipeline.py), and [`scripts/run_etl.py`](file:///e:/BDA%20ev%20charge/scripts/run_etl.py).

### Cleaning Reconciliation
- **Starting Raw Records:** `72,856`
- **Missing / Null Count:** `0` (clean CSV formatting)
- **Duplicate Rows Removed:** `13` exact duplicate rows
- **Invalid Records Filtered:** `735`
  - 17 records with negative duration (maintenance clock reversal anomalies)
  - 718 records with 0 duration (instantaneous disconnects without charging)
- **Final Cleaned Dataset:** **`72,108` sessions** (**98.97% retention rate**)
- **Storage:** Persisted as columnar Parquet at `data/processed/cleaned_charging_records.parquet`.

---

## 7. Cassandra Data Modeling & Query Design

Tables in `ev_analytics` are designed around specific dashboard queries using single-column partition keys for $O(1)$ point lookups:

```sql
-- 1. Portfolio Summary (1 row)
CREATE TABLE IF NOT EXISTS ev_analytics.overall_metrics (
    metric_id text,
    total_sessions bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    min_duration_minutes double,
    max_duration_minutes double,
    PRIMARY KEY (metric_id)
);

-- 2. Hourly Profile (24 rows)
CREATE TABLE IF NOT EXISTS ev_analytics.hourly_demand (
    start_hour int,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (start_hour)
);

-- 3. Charger Type Comparison (2 rows: AC, DC)
CREATE TABLE IF NOT EXISTS ev_analytics.charger_type_metrics (
    charger_current_type text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (charger_current_type)
);

-- 4. Location Categories (10 rows)
CREATE TABLE IF NOT EXISTS ev_analytics.location_metrics (
    location text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (location)
);
```

### Idempotence & Parity Verification
- **Upsert Semantics:** Running `scripts/load_cassandra.py` multiple times executes parameterized `INSERT` statements that update records in-place without duplicating rows.
- **Row Counts:** Verified strictly at 1, 24, 2, and 10 rows.
- **Data Parity:** Automated checks in [`scripts/check_cassandra_data.py`](file:///e:/BDA%20ev%20charge/scripts/check_cassandra_data.py) confirm 100% numerical match with source outputs.

---

## 8. Dashboard Implementation & Visual Interpretations

The dashboard in [`src/dashboard/app.py`](file:///e:/BDA%20ev%20charge/src/dashboard/app.py) runs on Streamlit and Plotly:

1. **Header & Data Source Badge:**
   - Displays `🟢 Connected to Cassandra` with keyspace and port details.
   - Includes a `🔄 Refresh Data` button clearing `@st.cache_data`.
2. **KPI Cards:**
   - Total Sessions: `72,108`
   - Total Energy: `1.27 GWh (1,267,066.08 kWh)`
   - Average Energy: `17.6 kWh`
   - Average Duration: `2h 33m (152.7 min)`
3. **Hourly Energy Demand Profile (By Session Start Hour):**
   - Line chart with area fill and peak callout annotation (`Peak: 118,980 kWh at 18:00`).
   - Grouped by the hour each session commenced (not instantaneous power flow).
4. **Charger Architecture Tabs (AC vs. DC):**
   - Visualizes session share (79.6% AC vs. 20.4% DC).
   - Dwell time comparison shows DC fast chargers have a **5.0x lower average session duration** (36.6 min vs. 182.5 min for AC).
5. **Location Siting Rankings:**
   - Horizontal bar chart with interactive sorting by volume, energy, or duration across 10 categories (`public area`, `apartment`, `resort`, `hotel`, `company`, etc.).
6. **Data-Driven Findings Container:**
   - Dynamically computed from query results without hardcoding.

---

## 9. Verification & Test Suite Summary

Executed via `.\.venv\Scripts\pytest.exe tests/`:
- **Scaffold Tests (`tests/test_scaffold.py`):** 4 tests passed.
- **ETL Tests (`tests/test_etl_pipeline.py`):** 7 tests passed (synthetic in-memory DataFrame).
- **Cassandra Mapping Tests (`tests/test_cassandra_mapping.py`):** 5 tests passed (offline type conversion).
- **Dashboard Helper Tests (`tests/test_dashboard_helpers.py`):** 5 tests passed (formatting and insights).
- **Overall Result:** **21 passed in ~61 seconds**.

---

## 10. Limitations & Future Scope

1. **Station Coordinates:** The public dataset categorizes locations by facility type (`apartment`, `company`) rather than providing exact GPS latitude/longitude (omitted by original authors for EV user privacy). Consequently, geographic map plotting is not feasible without synthetic data.
2. **Plugged Duration vs. Active Current Flow:** `Duration` measures total physical cable connection time. In residential destination charging, vehicles may remain connected overnight after the battery has reached 100%.
3. **Tariff Pricing:** The dataset does not include fluctuating electricity billing rates or transaction revenue.
4. **Future Extensions:** Real-time Kafka streaming integration for live telemetry simulation, machine learning charging completion forecasters, and multi-node Cassandra cluster deployment.

---

## 11. Complete Command Reference

```powershell
# 1. Start Apache Cassandra container
docker compose up -d

# 2. Verify environment components
.\.venv\Scripts\python.exe scripts/check_env.py

# 3. Verify Cassandra connectivity
.\.venv\Scripts\python.exe scripts/check_cassandra.py

# 4. Run PySpark ETL Pipeline
.\.venv\Scripts\python.exe scripts/run_etl.py

# 5. Load PySpark analytics into Cassandra
.\.venv\Scripts\python.exe scripts/load_cassandra.py

# 6. Verify Cassandra stored data parity
.\.venv\Scripts\python.exe scripts/check_cassandra_data.py

# 7. Execute complete unit test suite
.\.venv\Scripts\pytest.exe tests/

# 8. Launch Streamlit interactive dashboard
.\.venv\Scripts\streamlit.exe run src/dashboard/app.py --server.port 8501
```
