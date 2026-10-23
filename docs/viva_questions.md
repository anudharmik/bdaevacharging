# 30 Comprehensive Viva Questions & Concise Answers

**Project:** Electric Vehicle Charging Station Analytics using Apache Spark and Cassandra  
**Target Examination:** Big Data Analysis (BDA) Lab Viva  

---

### Part 1: Big Data Fundamentals & Motivation

#### Q1. What is the main objective of this project?
**Answer:** The objective is to build an end-to-end Big Data analytics pipeline that processes 72,856 real EV charging transaction sessions using Apache Spark, stores query-optimized analytical summaries in an Apache Cassandra NoSQL database, and visualizes them on an interactive Streamlit dashboard.

#### Q2. Why is EV charging data considered a Big Data problem?
**Answer:** As EV adoption grows, charging networks generate continuous high-frequency M2M telemetry (session start/end times, energy delivery, charger status). Analyzing millions of transactions across thousands of stations requires distributed data processing and scalable NoSQL persistence.

#### Q3. What real dataset is used in this project?
**Answer:** We use the real-world EV charging transaction dataset published in *Nature Scientific Data* (2024) and shared via the IEEE PES Data Sharing Task Force, containing 72,856 commercial sessions across one full year under a CC BY 4.0 license.

---

### Part 2: Apache Spark & PySpark Processing

#### Q4. Why did you use Apache Spark instead of standard Python or Pandas?
**Answer:** Pandas loads all data into a single machine's RAM and runs single-threaded. Apache Spark is a distributed computing engine that processes data in parallel partitions across multiple CPU cores or cluster worker nodes, scaling horizontally without memory crashes.

#### Q5. What is a SparkSession?
**Answer:** A `SparkSession` is the unified entry point for programming Spark with the DataFrame and Dataset APIs. It encapsulates the SparkContext, SQLContext, and configuration settings for local or cluster execution.

#### Q6. What is the difference between Spark Transformations and Actions?
**Answer:** 
- **Transformations** (e.g., `filter()`, `withColumn()`, `groupBy()`) are lazy; they build an execution plan (DAG) but do not execute immediately.
- **Actions** (e.g., `count()`, `show()`, `write()`, `collect()`) trigger the actual execution of the DAG to compute and return results.

#### Q7. Why did you declare an explicit `StructType` schema instead of using `inferSchema=True`?
**Answer:** `inferSchema=True` forces Spark to scan the entire 7.6 MB CSV file twice, degrading I/O performance. An explicit schema guarantees correct data types, prevents schema drift, and processes the file in a single pass.

#### Q8. What data cleaning steps were performed in PySpark?
**Answer:** 
1. Dropped 13 exact duplicate records using `dropDuplicates()`.
2. Parsed exact second-precision timestamps by combining `StartDay` and `StartTime`.
3. Filtered out 735 invalid sessions where `Duration <= 0` or `Demand <= 0`.
4. Mapped numeric `ChargerType` (0 -> AC, 1 -> DC) into human-readable text.

#### Q9. Why were 735 records removed during cleaning?
**Answer:** 17 records had negative durations caused by charger clock synchronization reversal anomalies during maintenance, and 718 records had 0 duration where a user connected and immediately unplugged before charging began.

#### Q10. How does Spark execute `groupBy()` and `agg()` under the hood?
**Answer:** Spark performs map-side pre-aggregation (partial sums and counts in executor memory) to minimize data movement, shuffles the partitioned keys across worker nodes, and performs a final reduce step to compute the overall metric.

#### Q11. In what format did you save the cleaned dataset, and why?
**Answer:** We saved the cleaned dataset as **Apache Parquet**. Parquet is a columnar, compressed, binary format that offers 5–10x faster query performance and significantly smaller disk footprint than CSV.

---

### Part 3: Apache Cassandra & NoSQL Data Modeling

#### Q12. Why did you choose Apache Cassandra instead of a relational database like MySQL?
**Answer:** Cassandra is a distributed, masterless NoSQL database designed for high-availability, horizontal scalability, and predictable single-digit millisecond reads and writes. It avoids single points of failure and excels at serving pre-aggregated time-series and analytical metrics.

#### Q13. What is a Keyspace in Cassandra?
**Answer:** A Keyspace is the outermost namespace in Cassandra, analogous to a database or schema in SQL. It defines data replication across cluster nodes (`SimpleStrategy` with `replication_factor: 1` in our local environment).

#### Q14. What is a Partition Key in Cassandra?
**Answer:** The Partition Key is the primary key column used by Cassandra's Murmur3Partitioner hash algorithm to determine which physical node stores that partition. It enables exact $O(1)$ point lookups.

#### Q15. Why does Cassandra use query-driven data modeling instead of 3NF normalization?
**Answer:** Cassandra does not support relational `JOIN` operations because joining large tables across distributed cluster nodes causes prohibitive network shuffle latency. Therefore, tables are designed specifically around the exact queries the application will execute.

#### Q16. Name the four Cassandra tables created in your project.
**Answer:**
1. `overall_metrics` (Primary key: `metric_id text`)
2. `hourly_demand` (Primary key: `start_hour int`)
3. `charger_type_metrics` (Primary key: `charger_current_type text`)
4. `location_metrics` (Primary key: `location text`)

#### Q17. What are Parameterized Queries and why are they used?
**Answer:** Parameterized queries use placeholders (`?`) prepared once via `session.prepare()`. They prevent CQL Injection vulnerabilities and allow Cassandra to cache the query execution plan for faster execution.

#### Q18. What happens when you run `load_cassandra.py` twice? Does it duplicate rows?
**Answer:** No. In Cassandra, `INSERT` statements are semantically **upserts** (insert or update). Because our tables use deterministic natural primary keys (e.g., `start_hour = 0..23`), reloading updates rows in-place without creating duplicates.

#### Q19. How did you verify schema compatibility before creating tables?
**Answer:** The client queries Cassandra's `system_schema.columns` to check that existing tables contain all expected columns. If an incompatible schema is detected, it halts with an informative error rather than destructively dropping the table.

---

### Part 4: Presentation Tier & Streamlit Dashboard

#### Q20. Why does the dashboard query Cassandra rather than the raw CSV or Spark?
**Answer:** Running a distributed Spark job on user clicks introduces seconds of latency and wastes compute resources. Cassandra serves pre-aggregated metrics in under 15 milliseconds, providing a responsive sub-second user experience.

#### Q21. Does the dashboard ever fall back to CSV files if Cassandra is down?
**Answer:** No. The dashboard strictly queries Cassandra. If Cassandra is unreachable, it displays an error banner with diagnostic commands (`docker compose up -d`) to ensure architectural integrity.

#### Q22. Why did you use Plotly instead of Matplotlib for visualizations?
**Answer:** Matplotlib generates static image files. Plotly produces interactive client-side charts that allow users to hover over data points to inspect exact values, zoom into time windows, and toggle chart series.

#### Q23. Why did you implement `@st.cache_data(ttl=60)`?
**Answer:** Streamlit re-executes the script on every user interaction (such as sorting a dropdown). Caching prevents unnecessary redundant queries to Cassandra within a 60-second window while keeping memory lightweight.

#### Q24. How is the 24-hour demand curve grouped?
**Answer:** It groups all 72,108 transactions by their **session start hour** (00:00 to 23:00). It represents the total energy demand demanded by sessions initiated in each hour, rather than continuous instantaneous grid flow.

---

### Part 5: Analytical Results, Insights & Limitations

#### Q25. What was the peak charging start window and total demand?
**Answer:** The peak session start window occurred at **18:00 (6:00 PM)** with **6,435 sessions** demanding **118,979.74 kWh**.

#### Q26. How do AC Level-2 and DC Fast Chargers compare in your findings?
**Answer:** 
- **AC Level-2**: Accounts for **79.6%** of sessions (57,398) with an average duration of **182.5 minutes (~3.0 hrs)**.
- **DC Fast Charging**: Accounts for **20.4%** of sessions (14,710) with an average duration of **36.6 minutes** (a **5.0x lower average session duration**).

#### Q27. Which location categories had the highest charging volume?
**Answer:** `public area` (13,952 sessions) and `apartment` complexes (13,913 sessions) combined represent over **38%** of all charging activity.

#### Q28. Why didn't you display a geographic map with charging station pins?
**Answer:** The original dataset authors deliberately omitted exact GPS latitude and longitude coordinates to protect EV driver privacy, providing only facility category classifications (`hotel`, `apartment`, `company`). We avoided generating fake GPS coordinates to maintain academic integrity.

#### Q29. Why didn't you include Machine Learning in this phase?
**Answer:** Machine learning models without real-time telemetry or vehicle battery state-of-charge (SoC) data would be superficial. Our project focuses on mastering core Big Data engineering: distributed ETL, NoSQL data modeling, and low-latency analytics.

#### Q30. What are two major limitations of this dataset?
**Answer:** 
1. It records total plugged-in connection duration, not active current flow duration (e.g., a car parked for 8 hours in an apartment may have finished charging in 2 hours).
2. It lacks electricity pricing or tariff rates to analyze revenue.
