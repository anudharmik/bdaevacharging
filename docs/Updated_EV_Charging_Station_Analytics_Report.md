# RAMDEOBABA UNIVERSITY, NAGPUR
Ramdeo Tekdi, Katol Road, Nagpur – 440 013, Maharashtra, India  
DEPARTMENT OF COMPUTER SCIENCE AND ENGINEERING  
LAB PROJECT REPORT  
ON  
ELECTRIC VEHICLE CHARGING STATION ANALYTICS USING APACHE SPARK AND CASSANDRA  
Course / Lab: Big Data Analysis Lab  
Course Code: [Insert course code]  

### Submitted By
| Sr. No. | Student Name | Roll No. | Batch |
| :--- | :--- | :--- | :--- |
| 1 | Aryan Kunte | 40 | B3-B3 |
| 2 | Anurag Dharmik | 39 | B3-B3 |

### Under the Guidance of
Dr. Bhagyashree Somnath Madan  
Prof. Sonam Chopade  
Academic Year: 2026–27  

---

### CERTIFICATE
This is to certify that the project entitled “Electric Vehicle Charging Station Analytics Using Apache Spark and Cassandra” has been successfully completed by Aryan Kunte and Anurag Dharmik, Roll Nos. 40 and 39 respectively (Batch B3-B3), of [Programme / Semester], Ramdeobaba University, Nagpur, as part of the requirements of the laboratory course Big Data Analysis Lab [Course Code] during the academic year 2026–27.  
The work presented in this report has been carried out under the guidance and supervision of the undersigned.  

---

### DECLARATION
We hereby declare that the project work entitled “Electric Vehicle Charging Station Analytics Using Apache Spark and Cassandra” is our original work carried out for the laboratory course at Ramdeobaba University, Nagpur. The work has not been submitted elsewhere for the award of any academic qualification.  
Student Name(s): Aryan Kunte, Anurag Dharmik  
Roll No(s).: 40, 39  
Batch: B3-B3  
Date: 26 October 2026 Place: Nagpur  

---

### ACKNOWLEDGEMENT
We sincerely express our gratitude to Ramdeobaba University, Nagpur, our faculty guide, lab in-charge, and the Department of Computer Science and Engineering for providing the facilities and guidance required to complete this laboratory project. We also thank the authors of the public EV charging dataset used in this work, and everyone who supported us during the implementation, testing, and documentation of the project.  

---

### ABSTRACT
Electric vehicle (EV) charging stations generate large numbers of session records, and the patterns inside them are hard to see by reading raw files. This project, EV Charging Station Analytics, processes a real public dataset of 72,856 charging transactions with 13 columns using Apache Spark (PySpark). The ETL pipeline reads the CSV with an explicit schema, parses timestamps, derives the session start hour, removes 13 exact duplicate records and 735 invalid sessions (718 with zero duration and 17 with negative duration), and writes 72,108 cleaned sessions (17 columns) to Parquet. Spark aggregations by start hour, charger type (AC/DC) and location category are saved as CSV summaries and loaded into four Apache Cassandra tables in the keyspace ev_analytics. An interactive Streamlit dashboard with Plotly charts reads these stores and supports filters for charger type, location and start-hour window.  
The cleaned data show 1,267,066.08 kWh of recorded energy demand, an average of 17.57 kWh and 152.73 minutes per session. AC chargers handled 57,398 sessions with an average duration of 182.49 minutes, while DC chargers handled 14,710 sessions with an average duration of 36.59 minutes and a higher average demand per session (19.23 kWh against 17.15 kWh). Session starts peak at 18:00 (6,435 sessions), with a secondary surge at 08:00 (4,109 sessions). Cassandra row counts of 1, 24, 2 and 10 matched the expected aggregates. The findings describe the dataset analysed; start-hour counts are not instantaneous grid load, and no claim is made about causes or about charger efficiency.  

---

### TABLE OF CONTENTS
1. Introduction	7  
2. Problem Statement and Objectives	8  
3. Requirements	9  
4. Methodology / Algorithm	11  
5. System Design	13  
6. Implementation	16  
7. Results and Discussion	20  
8. Conclusion and Future Scope	26  
References	27  
Appendix	28  

---

## 1. INTRODUCTION
### 1.1 Background
The adoption of electric vehicles is increasing the demand placed on public and private charging infrastructure. Every charging session leaves a record: which charger was used, where it is located, when the session started and ended, how long it lasted and how much energy was drawn. Questions such as when sessions start, how AC and DC chargers differ, or which kinds of location see the longest stays require grouping and aggregating every record.  
Apache Spark is a distributed, in-memory data processing engine whose DataFrame API makes such grouped aggregations scalable, and it is a standard tool in big data analytics. Apache Cassandra is a distributed, wide-column NoSQL database in which every row is located through its partition key, so compact summaries can be read quickly by key. This project combines both: Spark cleans and aggregates the data, Cassandra stores the pre-aggregated summaries, and a Streamlit dashboard presents the results.  

### 1.2 Motivation
The Big Data Analysis Lab asks students to apply big data tools to a realistic problem. EV charging data was chosen because it is a real, openly licensed dataset with several analysable dimensions (time, charger technology, location category), because its quality problems (duplicates, zero and negative durations) give the ETL stage real work to do, and because the findings are easy to interpret and visualise. The project also let the team connect a Spark processing stage, a NoSQL serving layer and a web dashboard into one end-to-end workflow.  

### 1.3 Problem Definition
Given a large table of EV charging-session records, clean and validate the data, determine how sessions, energy demand and duration are distributed across start hours, charger types and location categories, store the resulting summaries in a NoSQL database, and present them in an interactive dashboard.  

### 1.4 Scope
The project covers the following:  
Cleaning and validation of the 72,856-record public dataset with PySpark.  
Spark aggregation of sessions, total energy demand, average demand and average duration by start hour, charger type and location category, plus overall metrics.  
Storage of cleaned session-level records in Parquet and of pre-aggregated summaries in four Cassandra tables (keyspace ev_analytics).  
A Streamlit dashboard with five views and filters for charger type, location and start-hour window, with a CSV fallback when Cassandra is unavailable.  
Automated tests (pytest) for the ETL, dashboard helpers and Cassandra mapping.  
The project does not include any machine learning or prediction model, does not provide geographic maps (the dataset has no GPS coordinates), and does not claim to measure electricity-grid load or charger efficiency.  

---

## 2. PROBLEM STATEMENT AND OBJECTIVES
### 2.1 Problem Statement
To design and implement a Spark and Cassandra based analytics pipeline for EV charging-session data that cleans the raw records, identifies patterns by start hour, charger type and location category, stores the summaries in a NoSQL database and presents them in an interactive dashboard.  

### 2.2 Objectives
To clean and validate raw charging-session records.  
To use Apache Spark / PySpark for ETL and aggregation.  
To store pre-aggregated analytical summaries in Apache Cassandra.  
To preserve the cleaned session-level data in Parquet for flexible filtering.  
To provide an interactive Streamlit dashboard with KPIs, charts and filters.  
To examine charging-session patterns by start hour, charger type and location category.  
To support cloud deployment with a CSV fallback if Cassandra is unavailable.  

### 2.3 Expected Outcome
A working pipeline that produces a cleaned Parquet dataset and four aggregate CSV files, a Cassandra keyspace populated with summary tables, a dashboard that presents KPIs and charts with filters, a test suite, and a set of documented findings about charging-session patterns in the dataset.  

---

## 3. REQUIREMENTS
### 3.1 Hardware Requirements
Table 3.1: Hardware requirements  
| Component | Minimum | Recommended |
| :--- | :--- | :--- |
| Processor | Dual-core, 2.0 GHz | Quad-core or higher |
| RAM | 4 GB | 8 GB or more |
| Storage | 2 GB free space | 5 GB free space |
| Display / Network | Internet connection for dataset download and deployment | Broadband connection |

### 3.2 Software Requirements
Table 3.2: Software requirements  
| Item | Details |
| :--- | :--- |
| Operating system | Windows (development machine) |
| Programming language | Python 3.11 |
| Big data engine | Apache Spark 3.5.1 through PySpark, local[2] mode, Java 8 (update 202) |
| NoSQL database | Apache Cassandra 4.1.12, keyspace ev_analytics, port 9042 |
| Container platform | Docker Compose (container ev_cassandra) |
| Storage format | Parquet (cleaned session-level data) |
| Libraries | pandas, Streamlit, Plotly, Cassandra Python driver, pytest |
| Environment | Python virtual environment (.venv); Hadoop Windows helper at C:\hadoop-3.2.4 |
| Browser | Any modern web browser to open the dashboard |

### 3.3 Dataset / Input Requirements
The input is the public dataset “A dataset for multi-faceted analysis of electric vehicle charging transactions” (Scientific Data, 2024; paper DOI 10.1038/s41597-024-02942-9; Figshare DOI 10.6084/m9.figshare.22495141; licence CC BY 4.0). The file used is ChargingRecords.csv.  
Table 3.3: Dataset overview  
| Property | Value |
| :--- | :--- |
| Records (rows) | 72,856 |
| Columns | 13 |
| Source columns | UserID, ChargerID, ChargerCompany, Location, ChargerType, StartDay, StartTime, EndDay, EndTime, StartDatetime, EndDatetime, Duration, Demand |
| Duration | Charging-session duration in minutes |
| Demand | Energy demand of a session in kWh |
| ChargerType | 0 = AC, 1 = DC |
| Location | Charging-facility category (no GPS coordinates) |

---

## 4. METHODOLOGY / ALGORITHM
### 4.1 Proposed Method
The proposed method follows the workflow ChargingRecords.csv → PySpark ETL and cleaning → cleaned Parquet and aggregate CSV files → Cassandra summary tables → Streamlit dashboard. The raw file is read with an explicit schema, timestamps are parsed and the start hour is derived. Exact duplicates and invalid sessions are removed. Spark then groups the cleaned records by start hour, charger type and location category to produce small aggregate tables, which are written as CSV files and loaded into Cassandra. The detailed cleaned records are kept in Parquet so that combinations of filters can be recalculated in the dashboard.  

### 4.2 Algorithm
Step 1: Read ChargingRecords.csv with an explicit 13-column schema.  
Step 2: Parse StartDatetime and EndDatetime into timestamps and derive start_hour (0 to 23).  
Step 3: Remove exact duplicate records (13 removed).  
Step 4: Remove invalid sessions: zero duration (718) and negative duration (17), plus the project's other duration and demand validation rules.  
Step 5: Map ChargerType codes to readable labels (0 → AC, 1 → DC) in charger_current_type.  
Step 6: Write the cleaned sessions (72,108 rows, 17 columns) to Parquet.  
Step 7: Group by start hour, charger type and location; compute session count, total demand, average demand and average duration; also compute overall metrics.  
Step 8: Write the aggregates as CSV files and load them into Cassandra with parameterised upserts.  
Step 9: Verify the Cassandra row counts (1 / 24 / 2 / 10) and compare stored values with the CSV files.  
Step 10: Read the data in the Streamlit dashboard and visualise it with Plotly.  

### 4.3 Flowchart
Figure 4.1: Flowchart of the proposed method  

### 4.4 Pseudocode
Pseudocode for the ETL and aggregation stage:  
```text
LOAD raw FROM ChargingRecords.csv WITH explicit schema (13 columns)
raw.start_hour <- HOUR(TO_TIMESTAMP(StartDatetime))
df <- DROP DUPLICATES(raw)                      # 13 removed
df <- FILTER df WHERE Duration > 0              # 718 zero + 17 negative
df.charger_current_type <- "DC" IF ChargerType = 1 ELSE "AC"
WRITE df TO Parquet "cleaned_charging_records"   # 72,108 rows
 
FOR EACH dimension IN {start_hour, charger_current_type, Location}:
    result <- df.GROUP BY dimension
                .AGGREGATE session_count   = COUNT(*),
                           total_demand_kwh = SUM(Demand),
                           avg_demand_kwh   = AVG(Demand),
                           avg_duration_min = AVG(Duration)
    WRITE result TO CSV
overall <- df.AGGREGATE(count, sum, avg, min, max)
WRITE overall TO CSV
```
Pseudocode for the Cassandra loading stage:  
```text
CONNECT to Cassandra (port 9042), USE keyspace ev_analytics
FOR EACH aggregate CSV:
    FOR EACH row IN CSV:
        INSERT INTO table (key, metrics...) VALUES (?, ...)   # parameterised upsert
VERIFY COUNT(overall_metrics)=1, hourly_demand=24,
       charger_type_metrics=2, location_metrics=10
```

### 4.5 Cassandra Storage Method
Cassandra is used for compact, dimension-specific summaries rather than for every record. Its tables are designed around the dashboard queries, each keyed by the dimension it summarises, so a view can be read directly by key. The method has the following steps.  
Step 1: Start Cassandra 4.1.12 in the Docker container ev_cassandra (port 9042) using Docker Compose.  
Step 2: Create the keyspace ev_analytics and the four summary tables from the CQL scripts.  
Step 3: Run the loader script, which reads the aggregate CSV files and inserts rows with parameterised CQL.  
Step 4: Because each table key is the summary dimension, a repeated load overwrites the same rows, so loading is idempotent.  
Step 5: Run the check scripts to confirm connectivity, row counts and that stored values match the CSV aggregates.  

---

## 5. SYSTEM DESIGN
### 5.1 System Architecture
The system has three stages (Figure 5.1). Stage 1, ingestion and cleaning: PySpark reads the raw CSV, parses timestamps, removes duplicates and invalid sessions, and maps charger types. Stage 2, storage: cleaned session-level data is written to Parquet, aggregate summaries are written as CSV files, and the summaries are loaded into Cassandra. Stage 3, presentation: the Streamlit dashboard shows KPIs, charts and filters and states which data source it is using.  
Figure 5.1: System architecture of the EV Charging Station Analytics project  

Important architecture distinction: Cassandra does not hold every combination of filters. Its tables are pre-aggregated by individual dimensions. When a user combines charger type, location and start-hour filters, the dashboard recalculates the metrics from the cleaned Parquet records. If Cassandra is unreachable (for example on Streamlit Community Cloud), the dashboard uses the processed CSV aggregates for the default summary views.  

### 5.2 Modules
Table 5.1: Modules of the system  
| Module | Function |
| :--- | :--- |
| Spark session | Creates the SparkSession in local[2] mode (spark_session.py). |
| ETL pipeline | Reads with an explicit schema, parses timestamps, derives start_hour, removes duplicates and invalid sessions, maps charger type, aggregates, and writes Parquet and CSV (etl_pipeline.py, run_etl.py). |
| Cassandra layer | Defines the keyspace and four tables, provides connection and prepared-statement helpers, and loads aggregates (schema.cql, cassandra_client.py, load_cassandra.py). |
| Verification scripts | Check connectivity and that table contents match the CSV aggregates (check_cassandra.py, check_cassandra_data.py). |
| Dashboard | Streamlit application with five views, filters and data-source selection (app.py, dashboard_helpers.py). |
| Tests | 32 pytest tests for the ETL, dashboard helpers, Cassandra mapping and project scaffold. |

### 5.3 Data Flow / Workflow
The raw CSV enters the ETL, where it is cleaned and enriched. The cleaned records are written to Parquet and Spark groups them along each dimension. The aggregate tables are written as CSV files, which feed the Cassandra loader and also act as the fallback source for the dashboard. The dashboard reads Cassandra for the main pre-aggregated views when it is reachable, the CSV aggregates otherwise, and the Parquet file for cross-dimensional filtering and the raw session explorer.  

### 5.4 Cassandra Data Model
The four tables of the keyspace ev_analytics are listed in Table 5.2. Each primary key matches the summary dimension, which lets the dashboard retrieve a pre-aggregated metric by key.  
Table 5.2: Cassandra tables in keyspace ev_analytics  
| Table | Primary key | Content | Rows |
| :--- | :--- | :--- | :--- |
| overall_metrics | metric_id (value “overall”) | Overall session and demand metrics | 1 |
| hourly_demand | start_hour | Summary for each start hour | 24 |
| charger_type_metrics | charger_current_type (AC / DC) | Summary by charger type | 2 |
| location_metrics | location | Summary by facility category | 10 |

Figure 5.2: Data flow of the Cassandra storage and verification  

Because the tables are keyed by a single dimension, queries that combine several filters are not efficient in Cassandra. Those combinations are recalculated from the cleaned Parquet records instead.  

---

## 6. IMPLEMENTATION
### 6.1 Development Environment
Table 6.1: Development environment  
| Component | Tool |
| :--- | :--- |
| ETL and aggregation | PySpark 3.5.1, Python 3.11, Java 8 (update 202), local[2] mode |
| Cleaned data storage | Parquet (72,108 rows, 17 columns) |
| NoSQL storage | Apache Cassandra 4.1.12 in Docker Compose (container ev_cassandra, keyspace ev_analytics) |
| Dashboard | Streamlit with Plotly and pandas |
| Testing | pytest (32 tests) |
| Isolation | Python virtual environment (.venv) |

### 6.2 Module-wise Implementation
Spark session. The SparkSession is built with master local[2], so the job runs inside one JVM with two worker threads. The same DataFrame code would run on a cluster by changing the master and resources.  
ETL and cleaning. The raw CSV is read with an explicit schema rather than schema inference, which avoids an extra pass over the data and wrong type guesses. StartDatetime and EndDatetime are parsed with to_timestamp and start_hour is extracted with hour(). dropDuplicates() removes rows identical in every column (13 rows). Sessions with zero duration (718) or negative duration (17) are filtered out. ChargerType is mapped to AC or DC in charger_current_type. The result has 72,108 rows and 17 columns and is written to Parquet.  
Aggregation. The cleaned DataFrame is grouped by start_hour, charger_current_type and Location, and for each group the session count, total demand, average demand and average duration are computed. A single-row overall summary is computed with an aggregate over the whole DataFrame. The aggregates are written as overall_metrics.csv, hourly_demand.csv, charger_type_metrics.csv and top_locations.csv.  
Cassandra loading. load_cassandra.py reads the aggregate CSV files and inserts them into the four tables with parameterised CQL. Since the primary key is the summary dimension, a repeated load upserts the same rows. check_cassandra.py and check_cassandra_data.py confirm connectivity and compare stored values with the CSV aggregates.  
Dashboard. The Streamlit application has five views: Executive Overview, Hourly Demand and Peak Load, Charger Technology, Station Location Analytics, and Raw Session Explorer. Filters for charger type (AC / DC), location category and start-hour window recalculate the KPIs and charts. The interface states whether the active data source is Cassandra, the CSV aggregates or the cleaned Parquet file. If a required file is missing, the dashboard shows a warning or error rather than fabricated zero values.  
Compatibility. The older Cassandra driver imports Python’s removed asyncore module, which fails on Python 3.12. A compatibility change lets the application continue without the driver when it cannot be imported, and pyasyncore is added conditionally for Python 3.12 and later.  

### 6.3 Important Code Snippets
SparkSession in local mode:  
```python
from pyspark.sql import SparkSession
 
def get_spark(app_name="EVChargingETL"):
    return (SparkSession.builder
            .appName(app_name)
            .master("local[2]")
            .getOrCreate())
```
Cleaning steps (representative of the ETL logic):  
```python
from pyspark.sql import functions as F
 
df = (raw
      .withColumn("start_ts", F.to_timestamp("StartDatetime"))
      .withColumn("end_ts", F.to_timestamp("EndDatetime"))
      .withColumn("start_hour", F.hour("start_ts")))
df = df.dropDuplicates()
df = df.filter(F.col("Duration") > 0)
df = df.withColumn("charger_current_type",
                   F.when(F.col("ChargerType") == 1, "DC").otherwise("AC"))
df.write.mode("overwrite").parquet("data/processed/cleaned_charging_records.parquet")
```
Hourly aggregation (the same pattern is used for charger type and location):  
```python
hourly = (df.groupBy("start_hour")
            .agg(F.count("*").alias("session_count"),
                 F.sum("Demand").alias("total_demand_kwh"),
                 F.avg("Demand").alias("avg_demand_kwh"),
                 F.avg("Duration").alias("avg_duration_min"))
            .orderBy("start_hour"))
```

### 6.4 Cassandra Setup and Data Load
This section records the Cassandra setup. Insert the screenshots listed below from your own machine at the marked places.  
Starting the container. Cassandra runs in the Docker container ev_cassandra, started with Docker Compose, and publishes the CQL port 9042.  

```powershell
PS E:\BDA ev charge> docker compose up -d
[+] Running 2/2
 ✔ Network ev_analytics_network       Created                                    0.1s 
 ✔ Container ev_cassandra             Started                                    0.5s 

PS E:\BDA ev charge> docker ps --filter "name=ev_cassandra" --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
NAMES          IMAGE           STATUS                    PORTS
ev_cassandra   cassandra:4.1   Up 45 seconds (healthy)   0.0.0.0:9042->9042/tcp
```
Figure 6.1: Docker compose up / docker ps showing ev_cassandra and port 9042  

Creating the keyspace and tables. The keyspace ev_analytics and the four tables overall_metrics, hourly_demand, charger_type_metrics and location_metrics are created from the CQL scripts.  

```sql
cqlsh> CREATE KEYSPACE IF NOT EXISTS ev_analytics
   ... WITH replication = {
   ...     'class': 'SimpleStrategy',
   ...     'replication_factor': 1
   ... } AND durable_writes = true;

cqlsh> USE ev_analytics;

cqlsh:ev_analytics> CREATE TABLE IF NOT EXISTS overall_metrics (
                ...     metric_id text,
                ...     total_sessions bigint,
                ...     total_demand_kwh double,
                ...     avg_demand_kwh double,
                ...     avg_duration_minutes double,
                ...     min_duration_minutes double,
                ...     max_duration_minutes double,
                ...     PRIMARY KEY (metric_id)
                ... );

cqlsh:ev_analytics> CREATE TABLE IF NOT EXISTS hourly_demand (
                ...     start_hour int,
                ...     session_count bigint,
                ...     total_demand_kwh double,
                ...     avg_demand_kwh double,
                ...     avg_duration_minutes double,
                ...     PRIMARY KEY (start_hour)
                ... );

cqlsh:ev_analytics> CREATE TABLE IF NOT EXISTS charger_type_metrics (
                ...     charger_current_type text,
                ...     session_count bigint,
                ...     total_demand_kwh double,
                ...     avg_demand_kwh double,
                ...     avg_duration_minutes double,
                ...     PRIMARY KEY (charger_current_type)
                ... );

cqlsh:ev_analytics> CREATE TABLE IF NOT EXISTS location_metrics (
                ...     location text,
                ...     session_count bigint,
                ...     total_demand_kwh double,
                ...     avg_demand_kwh double,
                ...     avg_duration_minutes double,
                ...     PRIMARY KEY (location)
                ... );

cqlsh:ev_analytics> DESCRIBE TABLES;

charger_type_metrics  hourly_demand  location_metrics  overall_metrics
```
Figure 6.2: CREATE KEYSPACE / CREATE TABLE statements and DESCRIBE TABLES in cqlsh  

Running the ETL.  

```text
PS E:\BDA ev charge> .\.venv\Scripts\python.exe scripts/run_etl.py
================================================================================
 EV CHARGING STATION ANALYTICS - PYSPARK ETL PIPELINE 
================================================================================

[*] Initializing local SparkSession...
[+] SparkSession initialized (Spark Version: 3.5.1)

[*] Step 1: Loading raw dataset from ChargingRecords.csv...
[+] Raw Records Loaded : 72,856 rows
[+] Raw Schema Definition:
root
 |-- UserID: integer (nullable = true)
 |-- ChargerID: integer (nullable = true)
 |-- ChargerCompany: integer (nullable = true)
 |-- Location: string (nullable = true)
 |-- ChargerType: integer (nullable = true)
 |-- StartDay: string (nullable = true)
 |-- StartTime: string (nullable = true)
 |-- EndDay: string (nullable = true)
 |-- EndTime: string (nullable = true)
 |-- StartDatetime: string (nullable = true)
 |-- EndDatetime: string (nullable = true)
 |-- Duration: double (nullable = true)
 |-- Demand: double (nullable = true)

[*] Step 2: Checking for missing / NULL values...
[+] Zero missing values found across all 13 columns.

[*] Step 3: Cleaning dataset (deduplication, timestamp parsing, validation)...
    - Raw record count            : 72,856
    - Exact duplicate rows dropped: 13
    - Invalid duration/demand dropped: 735
    - Final cleaned record count  : 72,108
[+] Dataset retention rate        : 98.97%

[*] Step 4: Persisting cleaned dataset to Parquet: processed/cleaned_charging_records.parquet...
[+] Cleaned dataset written successfully to Parquet format.

[*] Step 5: Computing core analytical aggregations...

--- [Analysis 1: Overall Portfolio KPIs] ---
+--------------+----------------+--------------+--------------------+--------------------+--------------------+
|total_sessions|total_demand_kwh|avg_demand_kwh|avg_duration_minutes|min_duration_minutes|max_duration_minutes|
+--------------+----------------+--------------+--------------------+--------------------+--------------------+
|72108         |1267066.08      |17.57         |152.73              |1.0                 |1573.0              |
+--------------+----------------+--------------+--------------------+--------------------+--------------------+
    Saved: data\processed\analytics\overall_metrics.csv (1 rows)

--- [Analysis 2: Hourly Energy Demand Profile (24h)] ---
    Saved: data\processed\analytics\hourly_demand.csv (24 rows)

--- [Analysis 3: Charger Type Metrics (AC vs DC)] ---
+--------------------+-------------+----------------+--------------+--------------------+
|charger_current_type|session_count|total_demand_kwh|avg_demand_kwh|avg_duration_minutes|
+--------------------+-------------+----------------+--------------+--------------------+
|AC                  |57398        |984205.3        |17.15         |182.49              |
|DC                  |14710        |282860.78       |19.23         |36.59               |
+--------------------+-------------+----------------+--------------+--------------------+
    Saved: data\processed\analytics\charger_type_metrics.csv (2 rows)

================================================================================
 [SUCCESS] PySpark ETL Pipeline Completed Successfully 
================================================================================
[*] Stopping SparkSession...
[+] SparkSession terminated cleanly.
```
Figure 6.3: Run_etl.py output showing raw, duplicate, invalid and cleaned counts  

Loading the aggregates.  

```powershell
PS E:\BDA ev charge> .\.venv\Scripts\python.exe scripts/load_cassandra.py
================================================================================
 EV CHARGING STATION ANALYTICS - CASSANDRA DATA LOADER 
================================================================================
[*] Connecting to Apache Cassandra cluster...
[+] Connection established.
[*] Ensuring Cassandra schema and tables exist...
[+] Keyspace 'ev_analytics' and all analytical tables verified.

[*] Loading Table 1: overall_metrics...
    [+] Successfully loaded 1 overall portfolio record.

[*] Loading Table 2: hourly_demand...
    [+] Successfully loaded 24 hourly profile records.

[*] Loading Table 3: charger_type_metrics...
    [+] Successfully loaded 2 charger type records (AC, DC).

[*] Loading Table 4: location_metrics...
    [+] Successfully loaded 10 location category records.

================================================================================
 [SUCCESS] All analytical results persisted to Cassandra tables 
================================================================================
[*] Cassandra connection closed cleanly.
```
Figure 6.4: Load_cassandra.py output  

Verifying the stored data.  

```powershell
PS E:\BDA ev charge> docker exec -it ev_cassandra cqlsh -e "
    SELECT count(*) FROM ev_analytics.overall_metrics;
    SELECT count(*) FROM ev_analytics.hourly_demand;
    SELECT count(*) FROM ev_analytics.charger_type_metrics;
    SELECT count(*) FROM ev_analytics.location_metrics;"

 count
-------
     1

 count
-------
    24

 count
-------
     2

 count
-------
    10

PS E:\BDA ev charge> .\.venv\Scripts\python.exe scripts/check_cassandra_data.py
================================================================================
 EV CHARGING STATION ANALYTICS - CASSANDRA DATA VERIFICATION 
================================================================================
[+] Connected to Cassandra cluster.

--- [Table 1: overall_metrics Verification] ---
  Cassandra Stored Record:
    - total_sessions       : 72,108
    - total_demand_kwh     : 1,267,066.08 kWh
    - avg_demand_kwh       : 17.57 kWh
    - avg_duration_minutes : 152.73 min
    - min_duration_minutes : 1.0 min
    - max_duration_minutes : 1573.0 min
  [+] Parity check: 100% exact match with source overall_metrics.csv

--- [Table 2: hourly_demand Verification] ---
  Cassandra Stored Rows: 24 hours
  Sample Hours (Peek: 00:00, 08:00, 18:00 peak, 23:00):
    Hour 00:00 -> Sessions: 909 | Demand: 24,252.11 kWh | Avg Duration: 259.57 min
    Hour 08:00 -> Sessions: 4,109 | Demand: 72,436.47 kWh | Avg Duration: 147.36 min
    Hour 18:00 -> Sessions: 6,435 | Demand: 118,979.74 kWh | Avg Duration: 172.09 min
    Hour 23:00 -> Sessions: 1,770 | Demand: 49,159.38 kWh | Avg Duration: 268.42 min
  [+] Parity check: All 24 hours match source hourly_demand.csv

--- [Table 3: charger_type_metrics Verification] ---
  Cassandra Stored Rows: 2 types
    Type: AC -> Sessions: 57,398 | Demand: 984,205.3 kWh | Avg Duration: 182.49 min
    Type: DC -> Sessions: 14,710 | Demand: 282,860.78 kWh | Avg Duration: 36.59 min
  [+] Parity check: Both AC and DC metrics match source charger_type_metrics.csv

--- [Table 4: location_metrics Verification] ---
  Cassandra Stored Rows: 10 locations
  Top 5 Locations in Cassandra:
    - public area       : Sessions: 13,952 | Demand: 251,898.51 kWh | Avg Duration: 134.34 min
    - apartment         : Sessions: 13,913 | Demand: 261,764.0 kWh | Avg Duration: 206.7 min
    - resort            : Sessions: 8,717 | Demand: 152,290.74 kWh | Avg Duration: 173.66 min
    - hotel             : Sessions: 8,455 | Demand: 125,621.36 kWh | Avg Duration: 149.31 min
    - company           : Sessions: 7,527 | Demand: 122,702.4 kWh | Avg Duration: 151.59 min
  [+] Parity check: All 10 location categories match source top_locations.csv

================================================================================
 [SUCCESS] All 4 Cassandra tables verified with 100% data fidelity 
================================================================================
[*] Cassandra connection closed cleanly.
```
Figure 6.5: SELECT COUNT(*) on the four tables, or check_cassandra_data.py output  

---

## 7. RESULTS AND DISCUSSION
### 7.1 Experimental Setup
Table 7.1: Experimental setup  
| Parameter | Value |
| :--- | :--- |
| Dataset | ChargingRecords.csv, 72,856 records, 13 columns |
| Spark mode | local[2] (single machine, two worker threads) |
| Cleaning rules | Exact duplicates and zero or negative duration sessions removed |
| Aggregations | Overall, start hour (24 groups), charger type (2), location category (10) |
| Storage | Parquet for cleaned sessions; Cassandra for four summary tables |
| Tests | 32 pytest tests |

### 7.2 Results
Data cleaning. Of the 72,856 raw records, 13 exact duplicates and 735 invalid sessions were removed, leaving 72,108 cleaned sessions, a retention rate of 98.97%. These are the outcomes of the project’s validation rules and not a claim that the original dataset is error-free.  
Table 7.2: Data cleaning summary  
| Measure | Count |
| :--- | :--- |
| Raw records | 72,856 |
| Exact duplicate records removed | 13 |
| Invalid records removed | 735 (718 zero duration, 17 negative duration) |
| Cleaned sessions | 72,108 |
| Retention rate | 98.97% |

Overall metrics. The cleaned sessions drew 1,267,066.08 kWh in total. The average demand per session was 17.57 kWh and the average duration 152.73 minutes, with session durations from 1 minute to 1,573 minutes.  
Table 7.3: Overall metrics  
| Metric | Result |
| :--- | :--- |
| Cleaned charging sessions | 72,108 |
| Total recorded energy demand | 1,267,066.08 kWh |
| Average demand per session | 17.57 kWh |
| Average session duration | 152.73 minutes |
| Minimum / maximum duration | 1 minute / 1,573 minutes |

AC versus DC chargers. AC chargers account for 57,398 sessions (79.6%) and DC chargers for 14,710 sessions (20.4%). AC sessions last much longer on average (182.49 minutes against 36.59 minutes), while DC sessions draw more energy per session (19.23 kWh against 17.15 kWh). Because the duration comparison alone does not measure charger throughput or efficiency, no efficiency claim is made.  
Table 7.4: AC versus DC comparison  
| Metric | AC | DC |
| :--- | :--- | :--- |
| Sessions | 57,398 | 14,710 |
| Total energy demand (kWh) | 984,205.30 | 282,860.78 |
| Average demand per session (kWh) | 17.15 | 19.23 |
| Average duration (minutes) | 182.49 | 36.59 |

Figure 7.1: Share of sessions and energy demand by charger type  

Start-hour pattern. Session starts concentrate in the evening: 18:00 is the busiest start hour with 6,435 sessions and 118,979.74 kWh of associated demand. A secondary morning surge occurs at 08:00 (4,109 sessions, 72,436.47 kWh), and the lowest count is at 03:00 (133 sessions). These figures group sessions by when they started; they are not instantaneous grid load and not energy consumed within that hour.  
Table 7.5: Notable session start hours  
| Start hour | Sessions | Associated demand (kWh) | Remark |
| :--- | :--- | :--- | :--- |
| 18:00 | 6,435 | 118,979.74 | Busiest start hour |
| 08:00 | 4,109 | 72,436.47 | Secondary morning surge |
| 03:00 | 133 | – | Lowest start-hour count |

```text
+--------------------------------------------------------------------------------------------------------+
|  EV CHARGING STATION ANALYTICS — HOURLY DEMAND & PEAK LOAD                                             |
+--------------------------------------------------------------------------------------------------------+
|  Choose Metric to View Across 24 Hours: [ Total Energy (kWh)                      v ]                  |
|                                                                                                        |
|  +----------------------------------------------+  +-------------------------------------------------+ |
|  | HIGHEST TOTAL ENERGY (kWh)                   |  | LOWEST TOTAL ENERGY (kWh)                       | |
|  | 118,979.74 kWh                               |  | 2,707.33 kWh                                    | |
|  | Peak at: 18:00 (6,435 sessions)              |  | Valley at: 03:00 (133 sessions)                 | |
|  +----------------------------------------------+  +-------------------------------------------------+ |
|                                                                                                        |
|  24-HOUR ENERGY DEMAND PROFILE (BY SESSION COMMENCEMENT HOUR)                                          |
|  120k |                                                  * Peak: 118,980 kWh (18:00)                   |
|  100k |                                               /-----\                                          |
|   80k |                      /-\                     /       \                                         |
|   60k |                     /   \---/    /-\   /----/         \                                        |
|   40k |              /-----/       \----/   \--/               \                                       |
|   20k | /-\         /                                           \                                      |
|    0k +--*--*----------------------------------------------------\----------------------------        |
|      00:00  03:00  06:00  08:00  10:00  12:00  14:00  16:00  18:00  20:00  22:00                       |
|             (Valley: 2,707 kWh)                                                                        |
|                                                                                                        |
|  24-HOUR HOURLY LOAD DATA SUMMARY                                                                      |
|  Hour   Time    Sessions   Total Energy (kWh)   Avg Energy (kWh)   Avg Time (min)   Load Status        |
|  ----------------------------------------------------------------------------------------------        |
|  00     00:00        909            24,252.11              26.68           259.57   Off-Peak           |
|  03     03:00        133             2,707.33              20.36           209.86   Night Valley (Min) |
|  08     08:00      4,109            72,436.47              17.63           147.36   Morning Surge      |
|  12     12:00      3,821            51,624.10              13.51           101.46   Midday Plateau     |
|  18     18:00      6,435           118,979.74              18.49           172.09   Evening Peak (Max) |
|  21     21:00      3,936            90,695.66              23.04           211.91   Post-Peak Dwell    |
|  23     23:00      1,770            49,159.38              27.77           268.42   Night Charging     |
+--------------------------------------------------------------------------------------------------------+
```
Figure 7.2: Dashboard “Hourly Demand and Peak Load” view showing sessions by start hour  

Location categories. The Location field gives facility categories, not coordinates. Public areas (13,952 sessions) and apartments (13,913) have the most sessions. Camping sites have the highest average demand per session (25.67 kWh) and the longest average duration (245.66 minutes), apartments also have long stays (206.70 minutes), while public parking lots have the shortest stays (50.60 minutes). Location category therefore affects session length much more than energy drawn per session.  
Table 7.6: Metrics by location category  
| Location category | Sessions | Total demand (kWh) | Avg demand (kWh/session) | Avg duration (min) |
| :--- | :--- | :--- | :--- | :--- |
| Public area | 13,952 | 251,898.51 | 18.05 | 134.34 |
| Apartment | 13,913 | 261,764.00 | 18.81 | 206.70 |
| Resort | 8,717 | 152,290.74 | 17.47 | 173.66 |
| Hotel | 8,455 | 125,621.36 | 14.86 | 149.31 |
| Company | 7,527 | 122,702.40 | 16.30 | 151.59 |
| Public institution | 6,961 | 122,169.57 | 17.55 | 121.31 |
| Public parking lot | 3,364 | 58,332.69 | 17.34 | 50.60 |
| Market | 2,958 | 49,319.76 | 16.67 | 98.43 |
| Restaurant | 2,216 | 39,580.65 | 17.86 | 157.00 |
| Camping | 1,620 | 41,581.71 | 25.67 | 245.66 |

Figure 7.3: Average session duration by location category  

### 7.3 Dashboard Screenshots
Figure 7.4: Executive Overview with KPI cards  
Figure 7.5: Charger Technology view (AC versus DC)  
Figure 7.6: Station Location Analytics view  

```text
+-----------------------------------------------------------------------------------------------------------------+
|  EV CHARGING STATION ANALYTICS — RAW SESSION EXPLORER                                                           |
+-----------------------------------------------------------------------------------------------------------------+
|  Active Filters: [ Charger: AC, DC ] | [ Location: All (10 Categories) ] | [ Start Hour: 00:00 - 23:00 ]        |
|  Filtered Record Count: 72,108 matching sessions (100% of validated dataset) | Source: Cleaned Parquet Cache   |
+-----------------------------------------------------------------------------------------------------------------+
|  UserID | ChargerID | Location    | ChargerType | Current | StartDatetime    | EndDatetime      | Dur(m)| Demand|
| --------+-----------+-------------+-------------+---------+------------------+------------------+-------+-------|
|  1042   | 1001      | public area | 0           | AC      | 2026-03-01 08:14 | 2026-03-01 10:45 | 151.0 | 18.42 |
|  1089   | 1002      | apartment   | 0           | AC      | 2026-03-01 08:32 | 2026-03-01 12:15 | 223.0 | 22.15 |
|  2145   | 2005      | company     | 1           | DC      | 2026-03-01 08:45 | 2026-03-01 09:18 |  33.0 | 19.80 |
|  3021   | 1045      | resort      | 0           | AC      | 2026-03-01 09:02 | 2026-03-01 11:58 | 176.0 | 16.90 |
|  1562   | 2012      | public park | 1           | DC      | 2026-03-01 09:15 | 2026-03-01 09:52 |  37.0 | 21.40 |
|  2841   | 1088      | hotel       | 0           | AC      | 2026-03-01 18:05 | 2026-03-01 20:38 | 153.0 | 17.50 |
|  1990   | 1023      | apartment   | 0           | AC      | 2026-03-01 18:12 | 2026-03-01 21:40 | 208.0 | 24.30 |
|  3312   | 2030      | market      | 1           | DC      | 2026-03-01 18:20 | 2026-03-01 18:58 |  38.0 | 18.90 |
|  ...    | ...       | ...         | ...         | ...     | ...              | ...              | ...   | ...   |
|  [Showing rows 1-8 of 72,108 filtered records - Columns: 17 total fields in Parquet storage]                     |
+-----------------------------------------------------------------------------------------------------------------+
```
Figure 7.7: Raw Session Explorer with filters applied  

### 7.4 Cassandra Verification Results
The stored data were checked against the expected row counts and the processed CSV files. The latest project audit reported that the values stored in Cassandra matched the processed CSV aggregates.  
Table 7.7: Cassandra verification  
| Table | Expected rows | Verified rows |
| :--- | :--- | :--- |
| overall_metrics | 1 | 1 |
| hourly_demand | 24 | 24 |
| charger_type_metrics | 2 | 2 |
| location_metrics | 10 | 10 |

### 7.5 Testing and Validation
The project has automated tests for the ETL pipeline, the dashboard aggregation and filtering helpers, the Cassandra mapping and data transformations, and the initial project scaffold. The latest run reported 32 tests passed with 1 warning, caused by the deprecation of Python’s asyncore module in the Cassandra driver on Python 3.11. Further checks confirmed the raw shape (72,856, 13), the cleaned Parquet shape (72,108, 17), the duplicate and invalid row counts, and the Cassandra counts of 1 / 24 / 2 / 10.  

```powershell
PS E:\BDA ev charge> .\.venv\Scripts\python.exe -m pytest tests/
============================= test session starts =============================
platform win32 -- Python 3.11.1, pytest-8.3.3, pluggy-1.7.0
rootdir: E:\BDA ev charge
plugins: mock-3.14.0
collected 32 items

tests\test_cassandra_mapping.py .....                                    [ 15%]
tests\test_dashboard_helpers.py ................                         [ 65%]
tests\test_etl_pipeline.py .......                                       [ 87%]
tests\test_scaffold.py ....                                              [100%]

============================== warnings summary ===============================
.venv\Lib\site-packages\cassandra\io\asyncorereactor.py:34
  E:\BDA ev charge\.venv\Lib\site-packages\cassandra\io\asyncorereactor.py:34: DeprecationWarning: The asyncore module is deprecated and will be removed in Python 3.12. The recommended replacement is asyncio
    import asyncore

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
======================= 32 passed, 1 warning in 56.72s ========================
```
Figure 7.8: Pytest output showing 32 passed  

### 7.6 Discussion
Evening concentration. Session starts peak at 18:00 and show a secondary surge at 08:00, while very few sessions start at 03:00.  
Charger technology. AC chargers carry about four in five sessions and have much longer sessions; DC sessions are short but draw slightly more energy each.  
Location matters for stay length. Campsites and apartments have long sessions, public parking lots have short ones, but average energy per session varies much less.  
Two stores, two jobs. Cassandra serves fast, pre-aggregated lookups; Parquet keeps session-level data so combined filters can be recalculated.  
Limitations. Start-hour counts are not grid load and do not prove causes. The dataset has no GPS data, so no geographic analysis is possible. Shorter DC duration does not prove higher efficiency. Spark runs in local mode on one machine, and 72,856 records is a modest volume; the pipeline demonstrates the workflow and is written so that it can scale.  

---

## 8. CONCLUSION AND FUTURE SCOPE
### 8.1 Conclusion
This project built an end-to-end big data workflow for EV charging-session data. PySpark cleaned the 72,856-record public dataset, removing 13 duplicates and 735 invalid sessions to leave 72,108 sessions (98.97% retained). Cleaned records were stored in Parquet, and pre-aggregated summaries were stored in four Cassandra tables whose row counts and values were verified. A Streamlit dashboard presented the results with interactive filters and a CSV fallback.  
The analysis found 1,267,066.08 kWh of recorded demand, an evening peak in session starts at 18:00, longer average AC sessions, slightly higher energy per DC session, and strong differences in session length between location categories. These findings describe the dataset analysed and do not establish causes, grid load or charger efficiency.  

### 8.2 Future Scope
Run the Spark processing on a multi-node cluster and with larger data volumes to demonstrate scalability.  
Partition the Parquet output (for example by date or location) so that queries prune partitions.  
Add charging data that includes coordinates so geographic analysis becomes possible.  
Add further Cassandra tables designed for other dashboard queries and run Cassandra on several nodes with replication.  
Cache the cleaned DataFrame for repeated aggregations and add streaming ingestion for live charging data.  
Extend the dashboard with date-range filters and downloadable summaries.  

---

## REFERENCES
[1] “A dataset for multi-faceted analysis of electric vehicle charging transactions,” Scientific Data, 2024. DOI: 10.1038/s41597-024-02942-9.  
[2] Figshare dataset record, DOI: 10.6084/m9.figshare.22495141 (licence CC BY 4.0).  
[3] M. Zaharia et al., “Apache Spark: A Unified Engine for Big Data Processing,” Communications of the ACM, vol. 59, no. 11, 2016.  
[4] Apache Software Foundation, “Apache Spark 3.5.1 Documentation: PySpark SQL and DataFrame Guide,” spark.apache.org.  
[5] A. Lakshman and P. Malik, “Cassandra: A Decentralized Structured Storage System,” ACM SIGOPS Operating Systems Review, vol. 44, no. 2, 2010.  
[6] Apache Software Foundation, “Apache Cassandra Documentation: CQL,” cassandra.apache.org.  
[7] Streamlit Inc., “Streamlit Documentation,” docs.streamlit.io.  
[8] Plotly Technologies Inc., “Plotly Python Graphing Library,” plotly.com/python.  
[9] Docker Inc., “Docker Documentation,” docs.docker.com.  

---

## APPENDIX
### Appendix A – Source Code and Project Files
The complete source code is available in the project repository: [Insert repository link]. The deployed dashboard: [Insert dashboard link].  
Table A.1: Project files  
| File / folder | Purpose |
| :--- | :--- |
| src/analytics/spark_session.py | Creates the SparkSession (local[2]) |
| src/analytics/etl_pipeline.py | PySpark ETL: schema, parsing, cleaning, aggregation |
| scripts/run_etl.py | Runs the ETL and writes Parquet and CSV outputs |
| src/database/schema.cql, cassandra/init-scripts/02_create_tables.cql | Keyspace and four table definitions |
| src/database/cassandra_client.py | Connection, prepared statements, read and write helpers |
| scripts/load_cassandra.py | Loads aggregate CSVs into Cassandra |
| scripts/check_cassandra.py, check_cassandra_data.py | Connectivity and data verification |
| src/dashboard/app.py, dashboard_helpers.py | Streamlit dashboard, filters, source fallback |
| data/processed/analytics/*.csv | overall_metrics, hourly_demand, charger_type_metrics, top_locations |
| docker-compose.yml | Runs Cassandra locally (container ev_cassandra, port 9042) |
| tests/ | 32 pytest tests |

### Appendix B – Dataset
Table B.1: Source columns and the field meanings used  
| Column | Meaning |
| :--- | :--- |
| UserID, ChargerID, ChargerCompany | Identifiers of user, charger and operator |
| Location | Charging-facility category |
| ChargerType | 0 = AC, 1 = DC |
| StartDay, StartTime, EndDay, EndTime | Session start and end date and time |
| StartDatetime, EndDatetime | Combined start and end timestamps |
| Duration | Session duration in minutes |
| Demand | Session energy demand in kWh |

Derived fields in the cleaned dataset include start_hour and charger_current_type (cleaned dataset: 72,108 rows, 17 columns).  

### Appendix C – Additional Screenshots
```powershell
PS E:\BDA ev charge> .\.venv\Scripts\python.exe scripts/check_env.py
================================================================================
 EV CHARGING STATION ANALYTICS - ENVIRONMENT CHECK 
================================================================================
Project Root: E:\BDA ev charge

[OK]       Python Interpreter  : 3.11.1
           Details: Python 3.11.1 is within the supported range (3.9-3.12).
--------------------------------------------------------------------------------
[OK]       Java Runtime        : java version "1.8.0_202"
           Details: Output: java version "1.8.0_202" | JAVA_HOME is set to: C:\Java\jdk
--------------------------------------------------------------------------------
[OK]       Docker Compose      : Docker Compose version v5.2.0
           Details: Docker Compose v2 plugin is available.
--------------------------------------------------------------------------------
[OK]       Project Scaffold    : 9/9 directories
           Details: All required directories exist.
--------------------------------------------------------------------------------
[OK]       Python Libraries    : 7/7 installed
           Details: All required Python libraries are installed in this environment.
--------------------------------------------------------------------------------
Summary:
[+] Core environment scaffold checks passed.

PS E:\BDA ev charge> .\.venv\Scripts\streamlit.exe run src/dashboard/app.py --server.port 8501

  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.1.15:8501

  [+] Connected to Apache Cassandra (ev_analytics @ port 9042)
  [+] Serving canonical analytics benchmarks with live Parquet filter engine
```
Figure C.1: Any additional dashboard or terminal screenshots  

