# Viva Examination Notes & Technical Guide: Apache Cassandra Integration

This guide provides clear, student-friendly, and viva-ready answers explaining Apache Cassandra concepts, data modeling decisions, and the database integration implemented in Phase 3.

---

## 1. Core Architecture: Roles of PySpark vs. Apache Cassandra

### Why are both PySpark and Cassandra used in this project?
In a modern Big Data architecture, compute and storage have distinct responsibilities:
- **Apache Spark (The Compute Engine)**:
  - Designed for **heavy distributed processing** (batch cleaning, parsing 72,856 timestamps, filtering anomalies, calculating aggregations).
  - Spark is an analytical engine, **not an operational database**. Querying a raw Parquet or CSV file directly on user clicks in a web dashboard requires launching a distributed Spark job, which takes seconds or minutes.
- **Apache Cassandra (The Serving & Storage Layer)**:
  - Designed for **ultra-fast, predictable, low-latency lookups** (single-digit milliseconds).
  - Once Spark computes the aggregations (hourly grid load, AC vs. DC metrics, location totals), Cassandra stores these pre-aggregated metrics. The web dashboard (Streamlit) queries Cassandra directly via primary keys with sub-second response times, without burdening Spark.

---

## 2. Cassandra Fundamentals: Keyspace, Tables, and Keys

### What is a Keyspace?
A **Keyspace** in Apache Cassandra is the top-level namespace that groups related tables, analogous to a database or schema in relational systems (e.g., MySQL or PostgreSQL).
- In our project, the keyspace is `ev_analytics`.
- It defines the replication strategy and replication factor:
  ```sql
  CREATE KEYSPACE IF NOT EXISTS ev_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  ```
  *(In single-node local development, `SimpleStrategy` with `replication_factor: 1` stores one copy of the data. In production clusters across multiple datacenters, `NetworkTopologyStrategy` would replicate data across racks and availability zones).*

---

### What is a Table in Cassandra?
A **Table** (historically called a Column Family) is an organized collection of partitions containing rows and typed columns. Unlike RDBMS tables, Cassandra tables do not support relational joins (`JOIN`), foreign keys, or unrestricted `WHERE` filters across non-key columns.

---

### What is a Partition Key vs. a Primary Key?
- **Partition Key**:
  - The first part of the Primary Key.
  - Cassandra hashes the partition key using the **Murmur3Partitioner** algorithm into an integer token between $-2^{63}$ and $2^{63}-1$.
  - This token determines which specific physical node in the cluster stores that partition.
  - *Example*: In `hourly_demand`, `start_hour` is the partition key. Data for hour `18` is placed on the partition token mapped to `18`.
- **Clustering Columns** (optional):
  - Any subsequent columns in a compound Primary Key (`PRIMARY KEY (partition_key, clustering_col)`).
  - They determine the sort order of rows *within* a physical partition on disk.
- **Primary Key in our Phase 3 Tables**:
  - Each of our four tables uses a single-column Primary Key that acts directly as the Partition Key for exact $O(1)$ point lookups.

---

## 3. Query-Driven Data Modeling

### Why are Cassandra tables designed around queries rather than 3NF normalization?
In relational databases (RDBMS), data is normalized (3rd Normal Form) to avoid redundancy, and complex queries join multiple tables at runtime.
- In distributed NoSQL systems like Cassandra, **distributed joins across cluster nodes are prohibitively expensive and slow** (requiring massive network shuffle of tables across nodes).
- Therefore, Cassandra follows **query-driven modeling**:
  - Rule: *First define the query your application needs, then design a dedicated table with a partition key matching that query's filter.*
  - In our project, each table directly answers one specific analytical question:
    1. `overall_metrics`: "Give me the total portfolio summary." (Key: `metric_id = 'overall'`)
    2. `hourly_demand`: "Give me the demand curve for each hour." (Key: `start_hour = 0..23`)
    3. `charger_type_metrics`: "Give me the metrics for AC vs. DC." (Key: `charger_current_type = 'AC' | 'DC'`)
    4. `location_metrics`: "Give me the metrics for a specific location." (Key: `location = 'apartment'`)

---

### Why store aggregated results rather than duplicating all 72,108 raw sessions?
1. **Application Requirements**: The dashboard needs to display executive metrics, hourly grid load charts, and station utilization. It does not need to repeatedly scan 72,000 raw rows to sum demand on every page refresh.
2. **Efficiency**: Reading 24 rows from `hourly_demand` takes **2 milliseconds**. Scanning and aggregating 72,000 raw rows on demand would consume substantial CPU and memory repeatedly.
3. **Storage Tiering**: Raw records and Parquet files reside on cold/warm storage (Data Lake/HDFS), while queryable analytical summaries reside in Cassandra for instant presentation.

---

## 4. Cassandra Operations & Technical Decisions

### What are Parameterized Queries and why use them?
In `src/database/cassandra_client.py`, statements are prepared once:
```python
statement = session.prepare("INSERT INTO hourly_demand (...) VALUES (?, ?, ?, ?, ?);")
session.execute(statement, [start_hour, count, demand, avg_demand, avg_duration])
```
**Two major benefits for viva:**
1. **Security**: Eliminates CQL Injection risks by strictly separating query structure from user data.
2. **Performance**: The Cassandra driver parses, plans, and caches the query on the server side once; subsequent executions simply transmit binary parameters without re-parsing CQL strings.

---

### Why does running `load_cassandra.py` twice NOT create duplicate rows?
In Cassandra, an `INSERT` statement is semantically an **UPSERT** (Insert or Update):
- If a row with the given Primary Key does not exist, Cassandra writes it.
- If a row with that Primary Key already exists, Cassandra overwrites the columns with the new timestamped values.
- Because our tables have deterministic, natural primary keys (`start_hour`, `charger_current_type`, `location`, `metric_id`), rerunning the loader 10 times will always result in exactly:
  - 1 row in `overall_metrics`
  24 rows in `hourly_demand`
  - 2 rows in `charger_type_metrics`
  - 10 rows in `location_metrics`

---

### What is the purpose of `verify_schema_compatibility()`?
In production data engineering, dropping tables automatically can cause catastrophic data loss.
- Our client inspects `system_schema.columns` before creating tables.
- If an existing table has columns incompatible with the expected schema, the code halts with an informative error rather than dropping the table silently (`DROP TABLE`).

---

## 5. Summary Table for Viva Quick Reference

| Table Name | Primary Key | Row Count | Query Access Pattern |
| :--- | :--- | :--- | :--- |
| `overall_metrics` | `metric_id` (`text`) | 1 | `SELECT * FROM overall_metrics WHERE metric_id = 'overall'` |
| `hourly_demand` | `start_hour` (`int`) | 24 | `SELECT * FROM hourly_demand WHERE start_hour = ?` (or full 24-hr scan) |
| `charger_type_metrics` | `charger_current_type` (`text`) | 2 | `SELECT * FROM charger_type_metrics WHERE charger_current_type = ?` |
| `location_metrics` | `location` (`text`) | 10 | `SELECT * FROM location_metrics WHERE location = ?` |
