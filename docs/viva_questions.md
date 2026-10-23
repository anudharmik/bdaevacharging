# Viva Examination Guide & Master Reference

**Project Title:** Electric Vehicle Charging Station Analytics using Apache Spark and Cassandra  
**Target Examination:** Big Data Analysis (BDA) Lab Viva / BTech Project Evaluation  
**Repository Location:** `E:\BDA ev charge`  

---

## A. Project Architecture

### End-to-End Pipeline Data Flow

```text
[Raw Transaction CSV]
data/raw/ChargingRecords.csv (72,856 rows, 13 columns, ~7.6 MB)
         │
         ▼
[Distributed PySpark Engine (Apache Spark 3.5.1)]
src/analytics/etl_pipeline.py
  ├── 1. Enforce explicit StructType schema (avoids double inferSchema scans)
  ├── 2. Exact deduplication: dropDuplicates() (drops 13 duplicate rows)
  ├── 3. Parse second-precision timestamps: concat_ws + to_timestamp
  ├── 4. Feature derivation: start_hour = hour(start_timestamp) (0..23)
  ├── 5. Current mode mapping: ChargerType 0 -> AC, 1 -> DC
  └── 6. Validation filter: Duration > 0 and Demand > 0 (drops 735 anomalies)
         │
         ▼
[Validated Clean Dataset]
data/processed/cleaned_charging_records.parquet (72,108 rows, ~8.0 MB)
         │
    ┌────┴───────────────────────────────────────┐
    ▼                                            ▼
[PySpark Analytical Aggregations]   [In-Memory Analytical Cache]
  - overall_metrics.csv (1 row)       - Fast multi-dimensional slicing
  - hourly_demand.csv (24 rows)       - Charger × Location × Hour Window
  - charger_type_metrics.csv (2 rows) - Pandas DataFrame (~8 MB RAM)
  - top_locations.csv (10 rows)                  │
  - duration_by_type.csv (2 rows)                │
    │                                            │
    ▼                                            │
[Apache Cassandra 4.1 Serving Layer]             │
Keyspace: ev_analytics | Port: 9042              │
  - overall_metrics (Partition: metric_id)       │
  - hourly_demand (Partition: start_hour)        │
  - charger_type_metrics (Partition: type)       │
  - location_metrics (Partition: location)       │
    │                                            │
    └─────────────────────┬──────────────────────┘
                          ▼
        [Streamlit + Plotly Operations Dashboard]
        src/dashboard/app.py | Port: 8501
          ├── Tab 1: Overview & Portfolio KPIs
          ├── Tab 2: Hourly Demand Profiling
          ├── Tab 3: Charger Comparison (AC vs DC)
          ├── Tab 4: Location & Siting Analysis
          └── Tab 5: Architecture & Provenance
```

### Component Roles & Viva Distinction:
1. **Raw Storage (Data Lake)**: Immutable CSV telemetry records collected over 12 commercial operating months.
2. **Apache Spark (Distributed Compute)**: Handles heavy batch ETL, deduplication, timestamp conversions, and distributed aggregation. Spark is **not** an operational serving database; it runs offline.
3. **Apache Cassandra (NoSQL Serving Layer)**: Serves pre-aggregated portfolio metrics to the web UI in sub-15ms latency using single-column partition keys, avoiding heavy Spark job launches on user clicks.
4. **Cleaned Parquet Cache (Multi-Dimensional Filter Engine)**: Cleaned session records cached in RAM with `@st.cache_data(ttl=300)` for real-time, responsive multi-attribute cross-filtering.
5. **Streamlit & Plotly (Presentation Tier)**: Python-native interactive UI delivering dynamic KPI recalculations, responsive dark charts, and live operational findings.

---

## B. Code Navigation Map

When the examiner asks to inspect the code, open files in this recommended sequence:

| Step | File Path | Key Functions / Items to Show | What to Explain to Examiner |
| :---: | :--- | :--- | :--- |
| **1** | `src/analytics/etl_pipeline.py` | `get_raw_schema()`, `clean_charging_data()`, `compute_overall_metrics()`, `compute_hourly_demand()` | Explicit schema declaration, duplicate removal, timestamp parsing, validation filtering, and distributed aggregations. |
| **2** | `src/database/schema.cql` | `CREATE KEYSPACE ev_analytics`, 4 `CREATE TABLE` statements | Query-driven NoSQL schema, primary keys, partition keys, and why tables match dashboard views. |
| **3** | `src/database/cassandra_client.py` | `get_cassandra_session()`, `create_tables()`, `insert_*()`, `fetch_*()` | Native transport connection, parameterized CQL inserts (upserts), and explicit dictionary query retrievals. |
| **4** | `scripts/load_cassandra.py` | `load_*_from_csv()`, `main()` | Reading PySpark analytics CSVs and loading them into Cassandra idempotently. |
| **5** | `src/dashboard/dashboard_helpers.py` | `filter_records()`, `compute_kpis_from_df()`, `aggregate_hourly_from_df()`, `get_metric_peak_valley()` | Exact mathematical formulas for KPIs, weighted averages (sum/count), 24h zero-fill ordering, and dynamic peaks. |
| **6** | `src/dashboard/app.py` | `load_data_from_cassandra()`, `load_cleaned_records()`, `st.sidebar`, 5 `st.tabs` | Dual data architecture, reactive sidebar filters, Plotly dark styling, and empty-state protection. |

---

## C. PySpark Code Walkthrough

### 1. Explicit Schema (`get_raw_schema`)
- Instead of using `inferSchema=True` (which forces two complete reads of the dataset across the network), an explicit `StructType` defines 13 fields (`UserID`, `ChargerID`, `ChargerCompany`, `Location`, `ChargerType`, `StartDay`, `StartTime`, `EndDay`, `EndTime`, `StartDatetime`, `EndDatetime`, `Duration`, `Demand`).
- Guarantees data type contract, prevents schema drift, and executes in a single pass.

### 2. Deduplication (`deduped_df = df.dropDuplicates()`)
- Eliminates 13 exact duplicate rows generated by M2M network retry packets.

### 3. Timestamp Parsing & Start-Hour Extraction
```python
with_features_df = (
    deduped_df.withColumn(
        "start_timestamp",
        to_timestamp(concat_ws(" ", col("StartDay"), col("StartTime")), "yyyy-MM-dd H:m:s")
    )
    .withColumn("end_timestamp", to_timestamp(concat_ws(" ", col("EndDay"), col("EndTime")), "yyyy-MM-dd H:m:s"))
    .withColumn("start_hour", hour(col("start_timestamp")))
    .withColumn(
        "charger_current_type",
        when(col("ChargerType") == 0, "AC")
        .when(col("ChargerType") == 1, "DC")
        .otherwise("UNKNOWN")
    )
)
```
- Concatenates date and time fields to capture second-level precision.
- Extracts `start_hour` (0..23) for 24-hour daily load profiling.
- Maps `ChargerType` codes: `0` $\to$ `AC` (Level 2), `1` $\to$ `DC` (Fast).

### 4. Validation Filter
```python
cleaned_df = with_features_df.filter(
    (col("Duration") > 0.0)
    & (col("Demand") > 0.0)
    & col("start_timestamp").isNotNull()
    & col("end_timestamp").isNotNull()
)
```
- Removes 735 anomalous rows:
  - 17 records with negative durations (clock synchronization reversals during maintenance).
  - 718 records with 0 duration (plugged in and immediately unlatched before energy transfer).
- Retains **72,108 clean transactions** (98.97% data retention rate).

### 5. Grouped Aggregations (`groupBy().agg()`)
- How it works: Spark performs **map-side pre-aggregation** (local partial sums and counts in worker memory), shuffles partition keys across the network, and performs a final reduce step.
- Outputs 5 core summary tables: `overall_metrics`, `hourly_demand`, `charger_type_metrics`, `top_locations`, `duration_by_type`.
- Cleaned session data is written to **Apache Parquet** (`data/processed/cleaned_charging_records.parquet`) for columnar compression and fast scan performance.

---

## D. Cassandra Code Walkthrough

### 1. Keyspace & Replication Strategy
```sql
CREATE KEYSPACE IF NOT EXISTS ev_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
} AND durable_writes = true;
```
- `SimpleStrategy` with `replication_factor: 1` is standard for single-node local development.
- In production across multi-datacenter clusters, `NetworkTopologyStrategy` replicates data across racks.

### 2. The Four Analytical Tables & Primary Keys

| Table Name | Primary Key | Key Type | Purpose & Dashboard Mapping |
| :--- | :--- | :--- | :--- |
| `overall_metrics` | `metric_id` | Partition Key (`'overall'`) | Stores 1 summary row for portfolio totals (sessions, total kWh, avg kWh, dwell bounds). Powers Tab 1 KPI cards. |
| `hourly_demand` | `start_hour` | Partition Key (`0..23`) | Stores 24 rows for arrival load windows. Powers 24h demand spline curve and Tab 2 deep-dive. |
| `charger_type_metrics` | `charger_current_type` | Partition Key (`'AC'`, `'DC'`) | Stores 2 rows comparing AC vs DC performance. Powers Tab 3 architectural comparisons. |
| `location_metrics` | `location` | Partition Key (facility name) | Stores 10 rows for facility installation categories. Powers Tab 4 siting leaderboard. |

### 3. Query-Driven Data Modeling
- In relational databases (RDBMS), data is normalized (3NF) and joined at runtime.
- In distributed NoSQL databases like Cassandra, **distributed table joins are prohibited** because shuffling tables across nodes introduces massive network latency.
- Therefore, Cassandra follows **query-driven modeling**: tables are modeled specifically to satisfy pre-determined application queries with $O(1)$ point lookups on their partition key.

### 4. Parameterized Inserts & Idempotent Upserts
- Queries in `src/database/cassandra_client.py` use prepared statements (`session.prepare(...)`).
- Benefit 1: **Security** — prevents CQL injection.
- Benefit 2: **Performance** — Cassandra parses the statement once and caches the execution plan.
- **Upsert Semantics**: Running `scripts/load_cassandra.py` multiple times overwrites rows in-place using deterministic natural keys (`start_hour`, `metric_id`), never creating duplicates.

### 5. Why Cassandra Is Not an Arbitrary Multi-Dimensional Engine
- Cassandra tables store 1-dimensional pre-aggregated projections (`start_hour` OR `charger_type` OR `location`).
- They do not store 3-way Cartesian intersections (`start_hour × charger_type × location`).
- For multi-dimensional ad-hoc slicing (e.g. *"DC fast chargers at apartments between 16:00 and 20:00"*), the dashboard transparently queries the in-memory cleaned Parquet cache.

---

## E. Dashboard Walkthrough & Interactive Filtering

### 1. Dual Data Architecture
- **Default State**: Serves canonical portfolio benchmarks directly from **Apache Cassandra 4.1**.
- **Active Filter Slice**: Slices the 72,108 validated session records from the in-memory Parquet cache (~8 MB).
- **Transparency Banner**: Explicitly displays the active data source:
  - `Source: Apache Cassandra 4.1 Serving Layer (Canonical Benchmark Aggregates)`
  - `Source: In-Memory Multi-Dimensional Filter Engine (Cleaned Parquet Cache)`

### 2. Mathematical Rigor in KPI & Average Calculations
- **Total Sessions**: $N = \sum 1$
- **Total Delivered Energy**: $E = \sum \text{Demand}_i$ (kWh)
- **Average Energy per Session**: $\bar{E} = \frac{E}{N}$ (kWh/session)
  - Calculated strictly as sum divided by count. **We never compute an unweighted average of averages**, avoiding Simpson's paradox.
- **Average Duration**: $\bar{D} = \frac{\sum \text{Duration}_i}{N}$ (minutes and formatted hours/minutes)
- **Observed Range**: $\min(\text{Duration})$ and $\max(\text{Duration})$
- **AC/DC Distribution**: $\frac{N_{\text{AC}}}{N} \times 100\%$ and $\frac{N_{\text{DC}}}{N} \times 100\%$
- **Active Slice Share**: $\frac{N_{\text{slice}}}{72,108} \times 100\%$

### 3. Caching & Reliability Layer
- `@st.cache_data(ttl=300)` caches both Cassandra queries and Parquet data in RAM for 5 minutes.
- User interactions (e.g., dragging the hour slider) re-render within milliseconds.
- `🔄 Clear Cache & Refresh` button calls `st.cache_data.clear()` to force fresh retrieval.
- **No Silent Fallbacks**: If Cassandra is offline, the dashboard displays a prominent error banner with troubleshooting commands (`docker compose up -d`). It never silently fabricates numbers.

### 4. Empty-State Protection
- If a filter combination has 0 matching records (e.g., DC chargers at camping sites at 03:00), the app displays a clear warning container rather than division-by-zero crashes or misleading zero charts.

---

## F. Verified Results & Operational Limitations

### Key Verified Analytical Results

| Metric / Question | Verified Value | Operational / Grid Insight |
| :--- | :--- | :--- |
| **Total Validated Transactions** | `72,108` sessions | Retained after conservative cleaning (748 anomalous rows removed). |
| **Total Delivered Energy** | `1,267,066.08 kWh` (~1.27 GWh) | Scale of electrical energy delivered across one annual commercial cycle. |
| **Average Energy / Session** | `17.57 kWh` | Typical passenger EV top-up quantity. |
| **Average Dwell Duration** | `152.73 min` (~2h 33m) | Range: 1.0 min to 1,573.0 min (26.2 hours). |
| **Peak Arrival Window** | `18:00 (6:00 PM)` | **6,435 sessions**, delivering **118,979.74 kWh**. Coincides with domestic residential peak. Candidate for Time-of-Use (ToU) demand management. |
| **Off-Peak Valley Window** | `03:00 (3:00 AM)` | **133 sessions**, delivering **2,707.33 kWh**. Ideal window for overnight fleet charging. |
| **AC Level 2 Chargers** | `57,398 sessions` (79.6%) | **182.49 min avg dwell** (~3.0 hrs). Destination charging (apartments, offices). |
| **DC Fast Chargers** | `14,710 sessions` (20.4%) | **36.59 min avg dwell** (19.23 kWh avg). **5.0x faster turnaround** for transit corridors. |
| **Top Siting Categories** | `public area` (13,952) & `apartment` (13,913) | Combine for **>38%** of total network charging volume. |

### Documented Operational Limitations
1. **No Station GPS Coordinates**: Original researchers omitted exact latitude and longitude to preserve EV driver privacy. Categorical classifications (`apartment`, `hotel`, etc.) are used instead. Fabricating coordinates would compromise research integrity.
2. **No Battery Telemetry**: Vehicle battery capacity (kWh) and State-of-Charge (SoC) are not captured in commercial billing records.
3. **Start-Hour Aggregation**: Hourly statistics group sessions by commencement time, modeling driver arrival patterns rather than instantaneous continuous power flow.

---

## G. 30 Comprehensive Viva Questions & Concise Answers

### Part 1: Big Data Fundamentals & Motivation

**Q1. What is the primary objective of this project?**  
*Answer:* To build an end-to-end Big Data pipeline processing 72,856 real EV charging sessions using Apache Spark for distributed ETL, Apache Cassandra for query-optimized NoSQL serving, and Streamlit/Plotly for interactive analytics.

**Q2. Why is EV charging considered a Big Data problem?**  
*Answer:* High-frequency M2M telemetry across expanding national networks generates continuous session logs. Processing, aggregating, and serving millions of sessions with low latency requires distributed compute and scalable NoSQL persistence.

**Q3. What dataset is used, and what is its provenance?**  
*Answer:* A real-world dataset published in *Nature Scientific Data* (2024) and curated by the IEEE PES Data Sharing Task Force, containing 72,856 genuine charging transactions across 12 months under a CC BY 4.0 license.

---

### Part 2: Apache Spark & PySpark Processing

**Q4. Why use Apache Spark instead of standard Pandas?**  
*Answer:* Pandas is single-threaded and bound to a single machine's RAM. Apache Spark is a distributed computing engine that processes partitioned DataFrames in parallel across CPU cores or cluster worker nodes, scaling horizontally without memory crashes.

**Q5. What is the difference between Spark Transformations and Actions?**  
*Answer:* Transformations (e.g., `filter()`, `withColumn()`, `groupBy()`) are lazy and build a Directed Acyclic Graph (DAG) execution plan. Actions (e.g., `count()`, `show()`, `write()`) trigger actual distributed execution of the DAG.

**Q6. Why declare an explicit `StructType` schema instead of `inferSchema=True`?**  
*Answer:* `inferSchema=True` requires Spark to scan the entire 7.6 MB CSV file twice, degrading performance. An explicit schema guarantees exact data types, eliminates schema drift, and processes the file in a single pass.

**Q7. What data cleaning steps were performed in PySpark?**  
*Answer:*  
1. Dropped 13 duplicate rows using `dropDuplicates()`.  
2. Combined `StartDay` and `StartTime` to parse second-precision timestamps.  
3. Extracted `start_hour` (0..23).  
4. Mapped `ChargerType` (0 $\to$ AC, 1 $\to$ DC).  
5. Filtered 735 invalid records where `Duration <= 0` or `Demand <= 0`.

**Q8. Why were 735 records removed during cleaning?**  
*Answer:* 17 records had negative durations caused by charger clock synchronization reversal anomalies during maintenance, and 718 records had 0 duration where a user connected and immediately disconnected before energy transfer.

**Q9. How does Spark execute `groupBy()` and `agg()` under the hood?**  
*Answer:* Spark performs map-side pre-aggregation in local executor memory (computing partial sums and counts), shuffles the partitioned keys across the network, and performs a final reduce step to compute the overall metric with minimal network I/O.

**Q10. Why did you save the cleaned dataset to Apache Parquet?**  
*Answer:* Parquet is a columnar, compressed binary format that provides 5–10x faster query performance, column pruning, and a substantially smaller disk footprint than CSV.

---

### Part 3: Apache Cassandra & NoSQL Modeling

**Q11. Why choose Apache Cassandra instead of MySQL or PostgreSQL?**  
*Answer:* Cassandra is a distributed, masterless NoSQL database designed for high write throughput, linear horizontal scalability, and predictable single-digit millisecond point lookups without single points of failure.

**Q12. What is a Keyspace in Cassandra?**  
*Answer:* A Keyspace is the top-level namespace in Cassandra, analogous to a database in SQL. It defines data replication strategies (`SimpleStrategy` with `replication_factor: 1` in our local environment).

**Q13. What is a Partition Key in Cassandra?**  
*Answer:* The partition key is hashed by Cassandra's Murmur3Partitioner algorithm to determine which physical node stores that partition, enabling exact $O(1)$ point lookups.

**Q14. Why does Cassandra use query-driven modeling instead of 3NF normalization?**  
*Answer:* Distributed joins across cluster nodes are prohibitively slow due to massive network shuffle. Therefore, tables are modeled specifically to satisfy pre-determined application queries without joins.

**Q15. Name the four Cassandra tables created in your project.**  
*Answer:* `overall_metrics` (key: `metric_id`), `hourly_demand` (key: `start_hour`), `charger_type_metrics` (key: `charger_current_type`), and `location_metrics` (key: `location`).

**Q16. What are Parameterized Queries and why are they used?**  
*Answer:* Parameterized queries (`?` placeholders) prepared via `session.prepare()` prevent CQL injection and enable Cassandra to cache query execution plans for faster performance.

**Q17. What happens when you run `load_cassandra.py` twice? Does it duplicate rows?**  
*Answer:* No. In Cassandra, `INSERT` statements are semantically **upserts** (insert or update). Using deterministic primary keys (e.g., `start_hour = 0..23`), re-running updates rows in-place without duplicating rows.

**Q18. How did you verify schema compatibility before creating tables?**  
*Answer:* The client queries `system_schema.columns` to verify that existing tables contain all expected columns. If an incompatible schema is detected, it raises a `RuntimeError` rather than destructively dropping tables.

---

### Part 4: Presentation Tier & Dashboard

**Q19. Why does the dashboard query Cassandra rather than Spark directly?**  
*Answer:* Running a Spark job on every UI click introduces seconds of latency. Cassandra serves pre-aggregated metrics in under 15 milliseconds, providing a responsive sub-second user experience.

**Q20. Does the dashboard ever fall back to CSV files if Cassandra is down?**  
*Answer:* No. The dashboard strictly queries Cassandra. If Cassandra is unreachable, it displays an error banner with diagnostic commands (`docker compose up -d`) to ensure architectural integrity.

**Q21. Why did you use Plotly instead of Matplotlib?**  
*Answer:* Matplotlib generates static raster images. Plotly produces interactive JavaScript charts allowing users to hover over data points, inspect exact values, and zoom into time windows.

**Q22. Why did you implement `@st.cache_data(ttl=300)`?**  
*Answer:* Streamlit reruns the Python script on every user interaction. Caching stores Cassandra queries and Parquet data in memory for 300 seconds, enabling instant widget updates without redundant database calls.

**Q23. How are KPI averages calculated when filters are applied?**  
*Answer:* All averages are strictly computed as total sum divided by session count ($\frac{\sum \text{Demand}}{N}$). We never average pre-computed averages, avoiding Simpson's paradox.

**Q24. Why does the dashboard use Parquet for cross-filtering instead of Cassandra?**  
*Answer:* Cassandra tables are pre-aggregated single-dimensional projections. Combined 3-way cross-filtering (e.g., DC at Apartments between 16:00 and 20:00) requires session-level records, which are served efficiently from the in-memory Parquet cache (~8 MB).

---

### Part 5: Analytical Results & Insights

**Q25. What was the peak session arrival window and energy demand?**  
*Answer:* The peak occurred at **18:00 (6:00 PM)** with **6,435 sessions** demanding **118,979.74 kWh**, coinciding with domestic residential evening peak grid load.

**Q26. What was the lowest demand window (valley)?**  
*Answer:* At **03:00 (3:00 AM)** with only **133 sessions** and **2,707.33 kWh**, representing optimal reserve capacity for overnight fleet charging.

**Q27. How do AC Level-2 and DC Fast chargers compare?**  
*Answer:* AC Level 2 accounts for **79.6%** of sessions with an average dwell of **182.5 minutes (~3.0 hrs)**. DC Fast accounts for **20.4%** of sessions with an average dwell of **36.6 minutes** (a **5.0x faster turnaround**).

**Q28. Which facility categories had the highest session volume?**  
*Answer:* `public area` (13,952 sessions) and `apartment` complexes (13,913 sessions) combined represent over **38%** of all network charging volume.

**Q29. Why is there no geographic map with station pins?**  
*Answer:* The original dataset authors omitted GPS coordinates to preserve EV driver privacy, providing categorical facility types instead. We avoided fabricating synthetic GPS coordinates.

**Q30. What is a key limitation of the dataset?**  
*Answer:* `Duration` records total connected cable time rather than active current flow time (e.g., an EV parked overnight in an apartment may finish charging in 2 hours but remain plugged in for 8 hours).

---

## H. Practical Demonstration Sequence (5–7 Minute Walkthrough)

Follow this structured sequence during your lab evaluation:

### Step 1: Prove Cassandra Service Is Running
Open PowerShell in `E:\BDA ev charge`:
```powershell
docker compose ps
```
*Show the examiner:* Container `ev_cassandra` has status `Up (healthy)` on port `9042`.

### Step 2: Demonstrate Cassandra Data Parity
```powershell
.\.venv\Scripts\python.exe scripts/check_cassandra_data.py
```
*Show the examiner:* All 4 Cassandra tables exist and match source PySpark analytics with 100% data fidelity:
- `overall_metrics`: 1 row (72,108 sessions, 1.27 GWh)
- `hourly_demand`: 24 rows (0..23 hours)
- `charger_type_metrics`: 2 rows (AC and DC)
- `location_metrics`: 10 rows

### Step 3: Run the Automated Unit Test Suite
```powershell
.\.venv\Scripts\pytest.exe tests/
```
*Show the examiner:* **32 passed tests** covering directory scaffold, PySpark ETL logic, Cassandra mapping, and dashboard filtering/KPI helpers.

### Step 4: Launch the Streamlit Operations Dashboard
```powershell
.\.venv\Scripts\streamlit.exe run src/dashboard/app.py --server.port 8501
```
Open `http://localhost:8501` in your browser.

### Step 5: Walk Through the Dashboard Features
1. **Header & Status**: Point out the live `● Connected to Cassandra` badge and keyspace `ev_analytics`.
2. **4 KPI Cards**: Point to Total Sessions (`72,108`), Total Energy (`1.27 GWh`), Avg Demand (`17.57 kWh`), and Avg Dwell (`2h 33m`).
3. **Interactive Filtering**:
   - In the sidebar, select **Charger Architecture: DC (Fast / Rapid)**.
   - Point out that sessions update to **14,710** (20.4% of portfolio), and average dwell immediately drops to **36.6 minutes**.
   - Change **Facility Category** to `Apartment`.
   - Adjust the **Start Hour Slider** to `16:00 – 20:00` (evening peak).
   - Point out the **Filter Transparency Banner** showing active criteria and in-memory cache source.
   - Click `All 24h` or reset filters to restore full portfolio view.
4. **5 Operational Tabs**:
   - Tab 1: 24h demand spline curve with peak annotation at 18:00 (118,980 kWh) and dynamic insight cards.
   - Tab 2: Toggle metric selector to *Average Session Duration* to show dwell curve.
   - Tab 3: Show head-to-head architectural table showing DC fast charging achieves **5.0x faster turnaround**.
   - Tab 4: Sort facilities by *Session Count* to show `public area` and `apartment` lead the network.
   - Tab 5: Show the end-to-end architecture diagram, Cassandra schema reference, and cleaning audit.
