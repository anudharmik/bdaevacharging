# COMPLETE PROJECT CONTEXT: EV CHARGING STATION ANALYTICS
### Apache Spark (PySpark) • Apache Cassandra (Docker) • Parquet • Streamlit • Plotly

> **Note for ChatGPT / Evaluator:**
> This document contains the complete context, architectural rationale, system design, Docker configuration, data modeling, end-to-end Python/PySpark/CQL code, operational findings, and viva exam preparation for the Big Data Analysis (BDA) Lab Project: **"Electric Vehicle Charging Station Analytics using Apache Spark and Cassandra"**.
> Use this context to explain any aspect of the project, generate presentation talking points, or conduct an interactive viva mock exam.

---

## 1. PROJECT METADATA & EXECUTIVE SUMMARY

- **Project Title:** Electric Vehicle Charging Station Analytics using Apache Spark and Cassandra
- **Laboratory Course:** Big Data Analysis (BDA) Lab
- **Institution:** Ramdeobaba University, Nagpur (RBU)
- **Authors:** Aryan Kunte (Roll 40, Batch B3-B3) & Anurag Dharmik (Roll 39, Batch B3-B3)
- **Academic Year:** 2026–27
- **Primary Objective:** Build an end-to-end Big Data analytics pipeline that ingests a real-world scientific dataset of 72,856 EV charging transaction records, cleans and aggregates the data using distributed Apache Spark (PySpark), persists query-optimized analytical summaries into an Apache Cassandra NoSQL database running in Docker, and presents actionable insights via an interactive Streamlit and Plotly dashboard.

### Core Key Findings from the Cleaned Data (72,108 Sessions)
1. **Total Volume & Energy:** 72,108 valid transactions delivering **1,267,066.08 kWh (~1.27 GWh)** of electricity, with an average of **17.57 kWh** and **152.73 minutes (~2.5 hours)** per session.
2. **Evening Grid Peak:** Charging starts concentrate heavily at **18:00 (6,435 sessions, 118,979.74 kWh)**, aligning directly with after-work residential return hours, followed by a secondary morning commute surge at **08:00 (4,109 sessions, 72,436.47 kWh)**.
3. **Overnight Valley:** Lowest start activity is at **03:00 (133 sessions)**, making late night the prime window for grid valley-filling incentives.
4. **AC vs. DC Charger Dynamics:**
   - **AC (Slow / Level 2):** Dominates volume with **57,398 sessions (79.6%)**, with an average duration of **182.49 minutes (~3.0 hours)** and average energy of **17.15 kWh**.
   - **DC (Fast / Rapid):** Handles **14,710 sessions (20.4%)**, delivering higher average energy (**19.23 kWh**) in **36.59 minutes** — achieving a **5.0x faster vehicle turnaround**.
5. **Location Siting:** Public parking areas and apartment complexes drive over **38% of total sessions** (13,952 and 13,913 respectively). Campsites exhibit the longest average stays (**245.66 min**) and highest energy per session (**25.67 kWh**), while public parking lots exhibit rapid turnover (**50.60 min**).

---

## 2. REAL-WORLD DATASET SPECIFICATION

- **Source Paper:** *"A dataset for multi-faceted analysis of electric vehicle charging transactions"*, published in *Nature Scientific Data* (2024), DOI: [10.1038/s41597-024-02942-9](https://doi.org/10.1038/s41597-024-02942-9).
- **Portal Attribution:** IEEE Task Force on Data Sharing in Energy Systems.
- **Repository:** Figshare (DOI: `10.6084/m9.figshare.22495141`), License: CC BY 4.0.
- **File:** `data/raw/ChargingRecords.csv` (~7.6 MB).
- **Raw Dimensions:** Exactly **72,856 rows** and **13 columns**.

### The 13 Raw Columns & PySpark Data Types
| Column Name | Raw Type | PySpark Type | Semantic Description |
| :--- | :--- | :--- | :--- |
| `UserID` | Integer | `IntegerType` | Anonymized unique EV driver identifier |
| `ChargerID` | Integer | `IntegerType` | Physical charging station unit identifier |
| `ChargerCompany` | Integer | `IntegerType` | Charging network operator identifier |
| `Location` | String | `StringType` | Siting facility classification (e.g., `apartment`, `public area`, `hotel`, `resort`) |
| `ChargerType` | Integer | `IntegerType` | Current type: `0` = AC (Slow / Level 2), `1` = DC (Fast / Rapid) |
| `StartDay` | String | `StringType` | Commencement date (`YYYY-MM-DD`) |
| `StartTime` | String | `StringType` | Commencement time (`HH:mm:ss`) |
| `EndDay` | String | `StringType` | Completion date (`YYYY-MM-DD`) |
| `EndTime` | String | `StringType` | Completion time (`HH:mm:ss`) |
| `StartDatetime` | String | `StringType` | Combined start timestamp without seconds (`YYYY-MM-DD HH:mm`) |
| `EndDatetime` | String | `StringType` | Combined end timestamp without seconds (`YYYY-MM-DD HH:mm`) |
| `Duration` | Double | `DoubleType` | Cable connection dwell time in **minutes** |
| `Demand` | Double | `DoubleType` | Total energy delivered in **kilowatt-hours (kWh)** |

### Data Cleaning Reconciliation
- **Raw Sessions:** 72,856
- **Missing / Null Values:** 0 (clean CSV format)
- **Exact Duplicates Dropped:** **13 rows** (caused by IoT / M2M telemetry network packet retries)
- **Anomalous Sessions Dropped:** **735 rows**
  - **718 rows** with `Duration == 0.0` (plugged in and unlatched immediately without transferring power)
  - **17 rows** with `Duration < 0.0` (clock-synchronization reversal during station controller maintenance)
- **Final Cleaned Sessions:** **72,108 rows** (**98.97% retention rate**)
- **Storage:** Written as Snappy-compressed columnar Parquet: `data/processed/cleaned_charging_records.parquet` (17 columns, including derived `start_timestamp`, `end_timestamp`, `start_hour`, and `charger_current_type`).

---

## 3. ARCHITECTURAL DECISIONS & "WHY WE DID THIS"

### 1. Why Apache Spark (PySpark) instead of Pandas?
- **Distributed Computing & Scale:** Pandas is single-threaded and bound to the memory of a single machine. While 72,856 rows fits in memory, real utility networks log millions of transactions daily across cities. PySpark abstracts distributed memory across worker nodes using resilient distributed datasets (RDDs) and Catalyst optimizer-backed DataFrames.
- **Cluster Portability:** The DataFrame code written in `etl_pipeline.py` runs unmodified whether on `local[2]` (our development laptop) or on a 100-node Databricks / Amazon EMR cluster simply by pointing `master()` to YARN or Kubernetes.
- **Catalyst Optimizer & Tungsten Engine:** Spark optimizes execution plans (predicate pushdown, projection pruning, and whole-stage code generation) before executing lazy transformations.
- **Explicit Schema Enforcement:** Instead of `spark.read.option("inferSchema", "true")` (which forces Spark to make two full passes over the file across the network), we provide an explicit `StructType` schema. This executes in a single pass, enforces type safety, and prevents schema drift.

### 2. Why Apache Cassandra? Why NoSQL instead of Relational (MySQL / PostgreSQL)?
- **High Write Throughput & Masterless Ring:** Cassandra uses a decentralized, peer-to-peer ring architecture with no master node (no single point of failure). Any node can accept reads or writes.
- **Query-Driven Data Modeling (Chebotko Methodology):** In relational databases, you normalize tables into 3NF and perform expensive `JOIN` operations at runtime. In Cassandra, you design tables **specifically around the application queries**. You pre-aggregate or denormalize data so queries are satisfied by reading a single partition key in $O(1)$ time.
- **Sub-15ms Latency for Dashboard Serving:** When a user opens the Streamlit dashboard, we do not want to launch a heavy Spark job or scan millions of rows. Cassandra serves pre-aggregated analytical summaries (`overall_metrics`, `hourly_demand`, `charger_type_metrics`, `location_metrics`) instantly by partition key.
- **Linear Horizontal Scalability:** To scale Cassandra, you simply add more commodity servers to the cluster ring. Writes are appended sequentially to commit logs and Memtables before flushing to SSTables on disk.

### 3. Why the Dual Serving Architecture (Cassandra + Parquet Cache)?
- **The Challenge:** Cassandra tables are keyed by single dimensions (`start_hour`, `charger_current_type`, `location`). In Cassandra, arbitrary multi-attribute filtering (e.g. `SELECT * WHERE charger='AC' AND location='hotel' AND hour=18`) requires `ALLOW FILTERING`, which is a notorious anti-pattern that triggers full cluster scans and causes timeouts.
- **The Solution:**
  1. **Canonical Portfolio Benchmarks:** Served directly from **Apache Cassandra 4.1** for instant page loads.
  2. **Dynamic Multi-Dimensional Slicing:** When the user adjusts cross-filters in the sidebar (combining Charger Type + Location + Start Hour range), the dashboard dynamically recalculates metrics from an in-memory cached copy of the **Cleaned Parquet dataset** (72,108 rows, ~8 MB in RAM) using `@st.cache_data`.
  3. **Fallback Resiliency:** If Cassandra is unreachable (e.g. running in cloud hosting without Docker), the dashboard seamlessly falls back to the processed CSV aggregates.

### 4. Why Columnar Parquet Format?
- Columnar layout stores values of the same column together, enabling high compression ratios with Snappy.
- Enables **column pruning** (reading only the required 4 columns instead of all 17) and **predicate pushdown** (skipping file row groups based on min/max statistics).

---

## 4. HOW WE CONNECTED CASSANDRA WITH DOCKER

### Container Configuration (`docker-compose.yml`)
```yaml
services:
  cassandra:
    image: cassandra:4.1
    container_name: ev_cassandra
    restart: unless-stopped
    ports:
      # Native transport CQL binary port exposed to host
      - "9042:9042"
    environment:
      - CASSANDRA_CLUSTER_NAME=EVChargeCluster
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_RACK=rack1
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      # JVM memory limits tuned for development workstations:
      - MAX_HEAP_SIZE=1024M
      - HEAP_NEWSIZE=256M
    volumes:
      # Persistent named volume for commit logs and SSTables
      - cassandra_data:/var/lib/cassandra
      # Mount directory for initial schema execution
      - ./cassandra/init-scripts:/init-scripts:ro
    healthcheck:
      test: ["CMD-SHELL", "cqlsh -e 'DESCRIBE KEYSPACES;' || exit 1"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 45s
    networks:
      - ev_network

volumes:
  cassandra_data:
    name: ev_cassandra_data

networks:
  ev_network:
    name: ev_analytics_network
    driver: bridge
```

### Key Technical Explanations for Docker & Cassandra:
1. **Port 9042:** The native binary transport port used by CQL clients and the DataStax Python driver (distinct from port 7000 used for inter-node gossip and port 7199 used for JMX monitoring).
2. **`MAX_HEAP_SIZE=1024M` & `HEAP_NEWSIZE=256M`:** By default, Cassandra attempts to consume 25% to 50% of total host RAM. On a developer laptop with 8GB or 16GB RAM, this can cause out-of-memory crashes or Windows freeze. Constraining the Java heap to 1GB ensures smooth coexistence with Spark.
3. **`GossipingPropertyFileSnitch`:** Informs Cassandra about rack and datacenter placement via gossip protocols, essential for production-grade network topology.
4. **Persistent Named Volume (`ev_cassandra_data`):** Ensures that when the container is stopped (`docker compose down`) or updated, the keyspace, table schemas, and loaded data remain permanently stored on the host filesystem.
5. **Startup Latency & Healthcheck (`start_period: 45s`):** Cassandra requires 30 to 45 seconds to initialize JVM memory buffers, load commit logs, and establish the native transport server. The healthcheck periodically runs `DESCRIBE KEYSPACES;` until the status changes from `(health: starting)` to `(healthy)`.

### Driver Connection in Python (`src/database/cassandra_client.py`)
```python
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
cluster = Cluster(
    contact_points=["localhost"],
    port=9042,
    auth_provider=auth_provider,
    connect_timeout=10,
)
session = cluster.connect("ev_analytics")
```
- **Asyncore Compatibility Note:** The DataStax Cassandra driver imports Python's deprecated `asyncore` module. On Python 3.11, this triggers a benign deprecation warning (`32 passed, 1 warning`). On Python 3.12+, `pyasyncore` is conditionally added to prevent runtime errors.

---

## 5. CASSANDRA DATA MODELING & CQL SCHEMA

### Keyspace Creation (`cassandra/init-scripts/01_create_keyspace.cql`)
```sql
CREATE KEYSPACE IF NOT EXISTS ev_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
} AND durable_writes = true;
```
- `SimpleStrategy` with `replication_factor: 1` is standard for single-node local development. In multi-datacenter production, `NetworkTopologyStrategy` is used.

### The Four Query-Driven Tables (`cassandra/init-scripts/02_create_tables.cql`)
```sql
USE ev_analytics;

-- Table 1: Overall Portfolio Summary (1 row)
-- Query: "Show executive KPIs (sessions, energy, dwell time bounds)"
CREATE TABLE IF NOT EXISTS overall_metrics (
    metric_id text,
    total_sessions bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    min_duration_minutes double,
    max_duration_minutes double,
    PRIMARY KEY (metric_id)
);

-- Table 2: Hourly Demand Profile (24 rows)
-- Query: "Show energy demand and session count for hour H (0..23)"
CREATE TABLE IF NOT EXISTS hourly_demand (
    start_hour int,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (start_hour)
);

-- Table 3: Charger Current Mode Metrics (2 rows: AC, DC)
-- Query: "Compare throughput and session duration of AC vs DC"
CREATE TABLE IF NOT EXISTS charger_type_metrics (
    charger_current_type text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (charger_current_type)
);

-- Table 4: Facility Siting Metrics (10 rows)
-- Query: "Retrieve session volume and duration for location category L"
CREATE TABLE IF NOT EXISTS location_metrics (
    location text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (location)
);
```

### Primary Key Design & Idempotence
- **Partition Key:** In each table, the partition key is the summary dimension itself (`metric_id`, `start_hour`, `charger_current_type`, `location`).
- **Upsert Semantics:** In Cassandra, `INSERT` statements are fundamentally upserts. Re-running `load_cassandra.py` overwrites the same partition keys in-place without generating duplicate rows.
- **Row Counts:** Verified strictly at **1, 24, 2, and 10 rows**.

---

## 6. COMPLETE PYSPARK ETL CODE & LOGIC

### 1. SparkSession Management (`src/analytics/spark_session.py`)
```python
import os
import sys
from pyspark.sql import SparkSession

def get_spark_session(
    app_name: str = "EVChargingStationAnalyticsETL",
    master: str = "local[2]",
    driver_memory: str = "1g",
) -> SparkSession:
    # Force Spark workers to use the active virtual environment Python
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Windows Hadoop winutils compatibility
    if not os.environ.get("HADOOP_HOME") and os.path.isdir(r"C:\hadoop-3.2.4"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop-3.2.4"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

### 2. PySpark ETL Pipeline (`src/analytics/etl_pipeline.py`)
```python
from typing import Dict, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg, col, concat_ws, count, hour,
    max as spark_max, min as spark_min,
    round as spark_round, sum as spark_sum,
    to_timestamp, when
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType,
    StructField, StructType
)

def get_raw_schema() -> StructType:
    """Explicit StructType avoiding inferSchema two-pass scans."""
    return StructType([
        StructField("UserID", IntegerType(), True),
        StructField("ChargerID", IntegerType(), True),
        StructField("ChargerCompany", IntegerType(), True),
        StructField("Location", StringType(), True),
        StructField("ChargerType", IntegerType(), True),
        StructField("StartDay", StringType(), True),
        StructField("StartTime", StringType(), True),
        StructField("EndDay", StringType(), True),
        StructField("EndTime", StringType(), True),
        StructField("StartDatetime", StringType(), True),
        StructField("EndDatetime", StringType(), True),
        StructField("Duration", DoubleType(), True),
        StructField("Demand", DoubleType(), True),
    ])

def clean_charging_data(df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    raw_count = df.count()

    # Step 1: Drop exact duplicates
    deduped_df = df.dropDuplicates()
    deduped_count = deduped_df.count()
    duplicate_rows_removed = raw_count - deduped_count

    # Step 2: Timestamp parsing & feature engineering
    with_features_df = (
        deduped_df.withColumn(
            "start_timestamp",
            to_timestamp(concat_ws(" ", col("StartDay"), col("StartTime")), "yyyy-MM-dd H:m:s")
        )
        .withColumn(
            "end_timestamp",
            to_timestamp(concat_ws(" ", col("EndDay"), col("EndTime")), "yyyy-MM-dd H:m:s")
        )
        .withColumn("start_hour", hour(col("start_timestamp")))
        .withColumn(
            "charger_current_type",
            when(col("ChargerType") == 0, "AC")
            .when(col("ChargerType") == 1, "DC")
            .otherwise("UNKNOWN")
        )
    )

    # Step 3: Filter invalid records (Duration <= 0 or Demand <= 0)
    cleaned_df = with_features_df.filter(
        (col("Duration") > 0.0)
        & (col("Demand") > 0.0)
        & col("start_timestamp").isNotNull()
        & col("end_timestamp").isNotNull()
    )
    clean_count = cleaned_df.count()
    invalid_rows_removed = deduped_count - clean_count

    metrics = {
        "raw_count": raw_count,
        "duplicate_rows_removed": duplicate_rows_removed,
        "invalid_rows_removed": invalid_rows_removed,
        "cleaned_count": clean_count,
    }
    return cleaned_df, metrics

# --- Analytical Aggregations ---

def compute_overall_metrics(cleaned_df: DataFrame) -> DataFrame:
    return cleaned_df.select(
        count("*").alias("total_sessions"),
        spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
        spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
        spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        spark_round(spark_min("Duration"), 2).alias("min_duration_minutes"),
        spark_round(spark_max("Duration"), 2).alias("max_duration_minutes"),
    )

def compute_hourly_demand(cleaned_df: DataFrame) -> DataFrame:
    return (
        cleaned_df.groupBy("start_hour")
        .agg(
            count("*").alias("session_count"),
            spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
            spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("start_hour")
    )

def compute_charger_type_metrics(cleaned_df: DataFrame) -> DataFrame:
    return (
        cleaned_df.groupBy("charger_current_type")
        .agg(
            count("*").alias("session_count"),
            spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
            spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("charger_current_type")
    )

def compute_top_locations(cleaned_df: DataFrame, limit_n: int = 10) -> DataFrame:
    return (
        cleaned_df.groupBy("Location")
        .agg(
            count("*").alias("session_count"),
            spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
            spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        )
        .orderBy(col("session_count").desc())
        .limit(limit_n)
    )
```

---

## 7. CASSANDRA LOADER & DATA VERIFICATION CODE

### Idempotent Loader (`scripts/load_cassandra.py`)
```python
import csv
from pathlib import Path
from src.database.cassandra_client import (
    get_cassandra_session, create_tables,
    insert_overall_metrics, insert_hourly_demand,
    insert_charger_type_metrics, insert_location_metrics,
    shutdown_cassandra
)

def main():
    cluster, session = get_cassandra_session()
    create_tables(session)

    # Example: Loading hourly demand
    with open("data/processed/analytics/hourly_demand.csv", mode="r", encoding="utf-8") as f:
        rows = [
            {
                "start_hour": int(r["start_hour"]),
                "session_count": int(r["session_count"]),
                "total_demand_kwh": float(r["total_demand_kwh"]),
                "avg_demand_kwh": float(r["avg_demand_kwh"]),
                "avg_duration_minutes": float(r["avg_duration_minutes"]),
            }
            for r in csv.DictReader(f)
        ]
        insert_hourly_demand(session, rows)

    print("[SUCCESS] All analytical results persisted to Cassandra tables")
    shutdown_cassandra(cluster, session)
```

### Parameterized Insert Helper (`src/database/cassandra_client.py`)
```python
def insert_hourly_demand(session: Session, hourly_records: List[Dict[str, Any]]) -> None:
    query = """
    INSERT INTO hourly_demand (
        start_hour, session_count, total_demand_kwh, avg_demand_kwh, avg_duration_minutes
    ) VALUES (?, ?, ?, ?, ?);
    """
    prepared = session.prepare(query)
    for r in hourly_records:
        session.execute(
            prepared,
            (
                r["start_hour"],
                r["session_count"],
                r["total_demand_kwh"],
                r["avg_demand_kwh"],
                r["avg_duration_minutes"],
            ),
        )
```
- **Why Prepared Statements?** The CQL query string is compiled and parsed once by the Cassandra cluster coordinator; subsequent executions only send binary parameter values across the wire, avoiding CQL injection and minimizing query parsing overhead.

---

## 8. STREAMLIT OPERATIONS DASHBOARD (`src/dashboard/app.py`)

### Dashboard Structure
1. **Header & Data Source Badge:**
   - Displays `🟢 Connected to Cassandra` (or `🟠 Processed CSV Fallback`).
   - Shows active dataset volume (`72,108 cleaned sessions`).
2. **Multi-Dimensional Sidebar Filters:**
   - **Charger Architecture:** Multi-select (`AC`, `DC`).
   - **Facility Category:** Multi-select across 10 categories (`apartment`, `public area`, etc.).
   - **Start-Hour Slider:** Dual slider window (`00:00` to `23:00`).
3. **Five Operational Tabs:**
   - **Tab 1: Overview & KPIs:** 4 KPI cards (Total Sessions, Total Energy, Avg Energy, Avg Duration), 24h demand spline area chart, frequency bar chart, key operational findings cards.
   - **Tab 2: Hourly Demand Profiling:** Metric selector (Total Energy, Sessions, Avg Energy, Avg Dwell), 24h profile with peak annotation at 18:00, peak/valley cards, raw hourly data table.
   - **Tab 3: Charger Comparison (AC vs DC):** Summary comparison table, AC vs DC share bar charts, duration turnaround speedup ratio (DC is 5.0x faster).
   - **Tab 4: Location Siting Analysis:** Horizontal bar chart rankable by volume, energy, or duration, location leaderboard table with volume share %.
   - **Tab 5: Architecture & Provenance:** Full ASCII pipeline diagram, Cassandra schema reference, cleaning audit, privacy constraints (privacy omission of GPS).

---

## 9. STEP-BY-STEP TERMINAL EXECUTION SEQUENCE

```powershell
# 1. Start single-node Apache Cassandra 4.1 container
docker compose up -d

# 2. Verify container is healthy and port 9042 is open
docker ps --filter "name=ev_cassandra"

# 3. Verify local environment (Python 3.11, Java 8, Docker, Scaffold)
.\.venv\Scripts\python.exe scripts/check_env.py

# 4. Verify TCP socket connectivity to Cassandra
.\.venv\Scripts\python.exe scripts/check_cassandra.py

# 5. Run complete PySpark ETL pipeline (reads CSV, cleans, outputs Parquet + CSVs)
.\.venv\Scripts\python.exe scripts/run_etl.py

# 6. Load aggregate summaries into Cassandra tables
.\.venv\Scripts\python.exe scripts/load_cassandra.py

# 7. Check 100% numerical data parity between Cassandra and CSVs
.\.venv\Scripts\python.exe scripts/check_cassandra_data.py

# 8. Execute test suite (32 unit tests)
.\.venv\Scripts\python.exe -m pytest tests/

# 9. Launch Streamlit interactive web dashboard
.\.venv\Scripts\streamlit.exe run src/dashboard/app.py --server.port 8501
```

---

## 10. VIVA EXAMINATION & DEFENSE CHEATSHEET (TOP 25 Q&A)

### Q1: What is the main objective of this project?
> **Answer:** To process and analyze 72,856 real-world commercial EV charging transactions using distributed Apache Spark (PySpark), store low-latency pre-aggregated summaries in Apache Cassandra (Docker), and visualize grid demand patterns and charger utilization via an interactive Streamlit dashboard.

### Q2: Why did you use Apache Spark instead of Pandas?
> **Answer:** Pandas is constrained to single-node memory and single-threaded execution. Real EV charging networks produce millions of telemetry records daily across power grids. PySpark provides a distributed DataFrame API with lazy evaluation and Catalyst execution optimization. The exact same PySpark pipeline code runs locally on our laptop or scales to a multi-node Spark cluster without modification.

### Q3: What is lazy evaluation in Apache Spark?
> **Answer:** Spark transformations (like `.filter()`, `.withColumn()`, `.groupBy()`) are not executed immediately. Instead, Spark constructs a Directed Acyclic Graph (DAG) of logical execution plans. Spark only triggers actual distributed computation when an **action** (such as `.count()`, `.collect()`, `.show()`, or `.write.parquet()`) is called. This enables Catalyst to optimize pipelined stages and minimize network shuffling.

### Q4: Why did you specify an explicit schema instead of `inferSchema=True`?
> **Answer:** `inferSchema=True` forces Spark to execute two complete passes across the dataset—one pass to sample data and guess types, and a second pass to read data. An explicit `StructType` schema enforces a strict data contract, prevents schema drift, and executes in a single pass.

### Q5: How did you clean the raw dataset?
> **Answer:** From 72,856 raw records:
> 1. Dropped **13 exact duplicate rows** using `dropDuplicates()` (telemetry transmission retries).
> 2. Filtered out **735 anomalous rows**: 718 with zero duration (immediate unlatches) and 17 with negative duration (maintenance clock reversal).
> 3. Verified **0 null values** across all columns.
> 4. Retained **72,108 validated sessions** (98.97% retention rate).

### Q6: Why did you choose Apache Cassandra over MySQL or PostgreSQL?
> **Answer:** Cassandra is a masterless, distributed NoSQL wide-column store designed for high-throughput write scalability and constant-time $O(1)$ partition key lookups. Traditional relational databases rely on relational normalization and expensive runtime table `JOIN`s, which become bottlenecks under big data volumes. Cassandra uses Query-Driven Modeling to eliminate joins.

### Q7: What is Query-Driven Data Modeling in Cassandra?
> **Answer:** In relational databases, you model data first (3NF) and write queries later. In Cassandra, you design tables **specifically around the application queries**. Each table has a partition key matching the exact lookup query (`metric_id`, `start_hour`, `charger_current_type`, `location`), guaranteeing direct hash-ring lookups without full-table scans.

### Q8: What is a Partition Key in Cassandra?
> **Answer:** The partition key determines which node in the Cassandra cluster ring stores the row. Cassandra passes the partition key through the Murmur3Partitioner hash function to determine the 64-bit token mapping to a specific cluster node, enabling $O(1)$ point lookups.

### Q9: Why didn't you store all 72,108 raw transactions in Cassandra?
> **Answer:** Cassandra is an operational serving database, not an analytical data lake. Arbitrary multi-dimensional filtering across multiple non-key columns (e.g., filtering by charger type, location, and start hour simultaneously) is inefficient in Cassandra and requires `ALLOW FILTERING`, which triggers expensive multi-node scans. Instead, Cassandra serves pre-aggregated canonical benchmarks, while the cleaned Parquet cache handles interactive cross-filtering.

### Q10: How does the dashboard achieve both fast lookups and interactive filtering?
> **Answer:** Through a **dual-layer architecture**:
> 1. **Serving Layer:** Queries Cassandra 4.1 on port 9042 for baseline portfolio benchmarks.
> 2. **Filter Engine:** Caches the cleaned Parquet dataset in memory (`@st.cache_data`) for instant dynamic multi-attribute slicing when the user changes sidebar filters.

### Q11: How is Cassandra connected and run?
> **Answer:** Cassandra 4.1 runs inside a Docker container named `ev_cassandra` managed by Docker Compose. Port `9042` is mapped to the host. The DataStax Python driver connects via native TCP binary protocol using `Cluster(["localhost"], port=9042).connect("ev_analytics")`.

### Q12: Why are Cassandra heap sizes limited in `docker-compose.yml`?
> **Answer:** By default, Cassandra allocates up to 50% of total host RAM to its JVM heap. On a personal workstation with 8GB or 16GB RAM, this can exhaust memory and crash the system. Setting `MAX_HEAP_SIZE=1024M` and `HEAP_NEWSIZE=256M` ensures stable single-node development while leaving plenty of RAM for PySpark and Streamlit.

### Q13: What does idempotence mean in your data loader?
> **Answer:** `load_cassandra.py` uses parameterized CQL `INSERT` statements with fixed primary keys. Because Cassandra treats `INSERT` as an upsert (update-or-insert), executing the script multiple times updates the existing rows in-place rather than appending duplicates. The table row counts stay strictly at 1, 24, 2, and 10.

### Q14: What is the significance of the 18:00 peak?
> **Answer:** 18:00 is the peak start hour with 6,435 sessions and 118,979.74 kWh of demand. This reflects the evening residential return window when EV drivers arrive home from work and plug in, coinciding with peak domestic electrical grid demand.

### Q15: What is the operational difference between AC and DC chargers in your data?
> **Answer:** AC chargers handle 79.6% of sessions with a 182.5-minute average dwell time (~3 hours), suited for destination charging (apartments, workplaces). DC fast chargers account for 20.4% of sessions but have an average dwell time of just 36.6 minutes with higher average energy (19.23 kWh vs. 17.15 kWh for AC), providing a 5.0x faster vehicle turnover.

### Q16: Why did campsites have the highest average energy per session?
> **Answer:** Campsites averaged 25.67 kWh and 245.66 minutes per session. Drivers visiting recreational campsites travel longer highway distances, arriving with low battery states-of-charge and staying parked for extended periods.

### Q17: Why did public parking lots have the lowest duration?
> **Answer:** Public parking lots averaged 50.60 minutes per session. They feature rapid turnover and a higher proportion of quick top-up sessions compared to residential destination locations.

### Q18: What is Parquet and why is it preferred over CSV?
> **Answer:** Parquet is an open-source columnar storage format. It uses dictionary encoding and Snappy compression to reduce disk footprint by up to 75% compared to raw CSV. It allows readers to load only the required columns (column projection) and skip unneeded row groups based on file metadata (predicate pushdown).

### Q19: What is the purpose of prepared statements in Cassandra?
> **Answer:** Prepared statements compile and optimize the CQL query once on the coordinator node. Subsequent executions only transmit binary parameter values across the network, reducing network overhead, avoiding CQL injection vulnerabilities, and boosting write throughput.

### Q20: What replication strategy did you use and why?
> **Answer:** We used `SimpleStrategy` with `replication_factor: 1`. This is the recommended strategy for a single-node local development container. In a multi-datacenter production cluster, we would use `NetworkTopologyStrategy` with `replication_factor: 3` across multiple availability zones.

### Q21: What are the primary keys of the four Cassandra tables?
> **Answer:**
> 1. `overall_metrics`: `metric_id` (value: `'overall'`)
> 2. `hourly_demand`: `start_hour` (integers `0..23`)
> 3. `charger_type_metrics`: `charger_current_type` (`'AC'`, `'DC'`)
> 4. `location_metrics`: `location` (10 location category names)

### Q22: Why doesn't the project plot geographical maps?
> **Answer:** The public scientific dataset deliberately omitted exact GPS latitude/longitude coordinates to protect EV driver privacy. Locations are classified by facility categories (`apartment`, `company`, `public area`, etc.). Fabricating random GPS coordinates was rejected to maintain rigorous scientific data integrity.

### Q23: Does start-hour demand represent instantaneous grid power?
> **Answer:** No. Start-hour grouping categorizes sessions by the hour they commenced. Since average AC charging lasts ~3 hours, energy is consumed across subsequent hours. We clearly document this distinction to avoid misleading grid operators.

### Q24: How was test-driven validation implemented?
> **Answer:** We wrote 32 automated tests using `pytest` covering scaffold paths, PySpark ETL logic (using synthetic in-memory DataFrames), Cassandra driver data type mappings, and Streamlit mathematical helpers (weighted averages and peak detection). All 32 tests pass 100%.

### Q25: How would you scale this architecture to handle 100 million records?
> **Answer:**
> 1. **Compute:** Deploy the PySpark job on an Apache Spark cluster managed by Kubernetes or AWS EMR, partitioning the Parquet output by year/month/location.
> 2. **Ingestion:** Stream live telemetry through an Apache Kafka cluster into Spark Structured Streaming.
> 3. **Database:** Expand the Cassandra cluster to 6+ nodes across multiple racks with `NetworkTopologyStrategy` and replication factor 3.
> 4. **Caching:** Place Redis in front of Cassandra for high-frequency dashboard queries.

