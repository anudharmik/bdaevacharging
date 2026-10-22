# Viva Examination Notes & Technical Guide: PySpark EV Charging Analytics

This document provides clear, direct, and viva-ready explanations of the core Big Data concepts, architecture choices, and data engineering decisions implemented in Phase 2 of this project.

---

## 1. Core Big Data Engineering Concepts

### What does ETL mean?
**ETL** stands for **Extract, Transform, Load**:
- **Extract**: Reading raw data from external storage systems or file formats without altering the source. In our pipeline, Spark extracts 72,856 charging records from `data/raw/ChargingRecords.csv`.
- **Transform**: Cleaning, validating, casting types, and aggregating the data into business-ready metrics. We parse timestamps, drop 13 exact duplicates, filter 735 non-positive duration records, map charger types (AC/DC), and compute hourly demand distributions.
- **Load**: Writing the cleaned, structured data into high-performance analytical storage (Parquet in Phase 2, and Cassandra NoSQL tables in Phase 3).

---

### Why use Apache Spark instead of standard Python or Pandas?
1. **Distributed Computing & Scalability**:
   - Pandas runs in a single process and loads all data into host RAM. If dataset size exceeds RAM, Pandas crashes with `MemoryError`.
   - Apache Spark operates on a distributed master/worker model using **RDDs** (Resilient Distributed Datasets) and partitioned DataFrames. It can scale horizontally across a cluster of 100 machines without changing the core business logic.
2. **Lazy Evaluation & Catalyst Optimizer**:
   - Spark does not execute transformations immediately. Instead, it builds a Directed Acyclic Graph (**DAG**) of execution stages.
   - Spark's Catalyst Optimizer inspects the entire query plan, performs predicate pushdown (filtering rows as early as possible), prunes unused columns, and reorders operations for maximum memory and CPU efficiency.
3. **Fault Tolerance**:
   - If a node or executor crashes during a long-running batch job, Spark automatically re-computes only the lost partition from lineage graph history.

---

### What is a Spark DataFrame?
A **DataFrame** in Spark is a distributed collection of data organized into named columns, conceptually equivalent to a table in a relational database or a Pandas DataFrame, but with distributed processing optimizations:
- It uses the high-performance **Tungsten binary execution engine**, which operates directly on off-heap binary memory to avoid Java garbage collection overhead.
- Operations on DataFrames are declared using high-level relational expressions (`filter`, `groupBy`, `agg`, `select`) rather than raw low-level map/reduce functions.

---

### Why is an explicit schema used instead of schema inference?
In our pipeline, we explicitly declare:
```python
StructType([
    StructField("UserID", IntegerType(), True),
    StructField("ChargerID", IntegerType(), True),
    ...
    StructField("Duration", DoubleType(), True),
    StructField("Demand", DoubleType(), True)
])
```
**Three key reasons for viva:**
1. **Performance**: With `inferSchema=True`, Spark must scan the entire 7.6 MB CSV file twice—once to guess column types across all 72,856 rows, and a second time to load it. Explicit schema reduces I/O to a single pass.
2. **Data Quality & Contract Guarantee**: Schema inference can misinterpret string codes (e.g., zip codes or ID numbers with leading zeros) as integers, or infer double precision when integer is required.
3. **Pipeline Stability**: If a future batch contains corrupted rows or nulls in the first few records, inferred schemas can change unexpectedly between runs, breaking downstream operations.

---

## 2. Data Cleaning & Transformation Logic

### How each cleaning operation works in our pipeline:

1. **Exact Deduplication (`dropDuplicates()`)**:
   - Raw records contain 13 identical duplicate rows where every column matches. These occur due to network transmission retries in machine-to-machine (M2M) telemetry.
   - `dropDuplicates()` performs a distributed hash-based deduplication, keeping exactly one copy per unique tuple.

2. **Timestamp Parsing with Second-Level Precision**:
   - The CSV contains separate `StartDay` (`YYYY-MM-DD`) and `StartTime` (`HH:mm:ss`) columns, as well as a truncated `StartDatetime` (`YYYY-MM-DD HH:mm`).
   - We concatenate `StartDay` and `StartTime` with `concat_ws(" ", col("StartDay"), col("StartTime"))` and parse it using `to_timestamp(..., "yyyy-MM-dd H:m:s")`.
   - This preserves exact second-level timestamps for session analysis.

3. **Temporal Feature Extraction (`hour()`)**:
   - We extract `start_hour = hour(start_timestamp)` (values 0 through 23).
   - This enables grouping all 72,108 sessions into 24 discrete hourly grid consumption windows.

4. **Domain Categorization (`when().otherwise()`)**:
   - `ChargerType` in raw data is encoded as `0` or `1`.
   - Verified against physical session metrics:
     - `0` has an average session duration of **182.5 minutes (~3 hours)** and average delivery of 17.15 kWh -> **AC (Slow Charging / Level 2)**.
     - `1` has an average session duration of **36.6 minutes** and average delivery of 19.23 kWh -> **DC (Rapid / Fast Charging)**.
   - We map this to a human-readable column `charger_current_type` (`AC` or `DC`).

5. **Conservative Outlier & Anomaly Filtering**:
   - We remove records where `Duration <= 0` or `Demand <= 0`:
     - 17 records had negative durations (e.g. -29 minutes due to charger clock sync reversals).
     - 718 records had 0 duration (sessions where the connector was plugged and immediately unlatched before meaningful charge transfer).
   - Total invalid records removed: **735 rows (1.01% of dataset)**.
   - Cleaned dataset preserves **72,108 verified charging transactions (98.97% retention)**.

---

## 3. How `groupBy()` and `agg()` Work Under the Hood

### What happens inside Spark during `groupBy("start_hour").agg(...)`?
1. **Map-Side Pre-Aggregation (Partial Aggregation)**:
   - Each executor processes its local partition of rows.
   - Instead of sending every single row across the network, Spark computes local partial sums (`sum(Demand)`) and partial counts (`count(*)`) for each hour inside memory hash maps.
2. **Shuffle Stage**:
   - Spark uses a hash partitioner on `start_hour` to route rows with the same hour to the same executor across the network.
   - Because of pre-aggregation, the amount of data transferred over the network (shuffle I/O) is drastically reduced.
3. **Reduce Stage (Final Aggregation)**:
   - The destination executor sums up the partial sums and counts to produce the final 24 rows, then computes `round(avg_demand, 2)`.

---

## 4. Engineering & Viva Value: Why These Analyses Matter

| Analysis | Metric / Result | Power Grid & Business Significance |
| :--- | :--- | :--- |
| **Overall Portfolio** | 72,108 sessions, **1,267,066.08 kWh** (~1.27 GWh) delivered. | Establishes the macro scale of electricity demand handled by the charging network operator. |
| **Hourly Demand Profile** | Evening peak between **17:00 and 20:00** (peaking at 18:00 with 6,435 sessions and 118,979.74 kWh). | Demonstrates the "EV charging coincidence factor" overlapping with domestic evening grid peaks. Crucial for utility time-of-use (TOU) tariff design and grid transformer overload mitigation. |
| **AC vs. DC Split** | **AC**: 57,398 sessions (79.6%), avg duration 182.5 min.<br/>**DC**: 14,710 sessions (20.4%), avg duration 36.6 min. | AC chargers dominate total session count, serving destination parking (apartments, hotels). DC fast chargers provide rapid turnover (5x faster) with higher peak power delivery. |
| **Top Locations** | `public area` (13,952 sessions), `apartment` (13,913 sessions), `resort` (8,717 sessions). | Identifies high-utilization siting categories for commercial charger deployment and municipal urban planning. |
| **Duration Statistics** | AC max 1573 min (~26 hrs), DC max 1413 min (~23 hrs); DC avg 36.6 min. | Exposes "charger hogging" (EVs remaining parked long after battery is full), motivating idle fees or automatic notification systems. |

---

## 5. Dataset Limitations to Discuss in Viva

1. **Geographic Coordinates Omitted**: The public dataset categorizes locations by facility type (`apartment`, `hotel`, `company`) rather than providing exact GPS latitude/longitude (to preserve driver privacy).
2. **Station Idling vs. Active Flow**: Duration records total plugged-in duration. A vehicle plugged in for 8 hours in an apartment may have finished charging in 2 hours.
3. **No Dynamic Tariff Rates**: The dataset contains electricity demand (`Demand` in kWh) but does not record fluctuating billing prices or currency transactions.
