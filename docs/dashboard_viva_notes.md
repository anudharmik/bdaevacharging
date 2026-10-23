# Viva Examination Notes & Technical Guide: Interactive Dashboard

This document provides clear, viva-ready explanations of the presentation tier, Streamlit framework, Plotly visualizations, data caching mechanisms, interactive filtering architecture, and how the web dashboard interfaces with Apache Cassandra and the underlying cleaned analytical dataset.

---

## 1. Presentation Tier & Dual Data Architecture

### What is the role of Streamlit in this Big Data project?
- **Streamlit** is a Python-native web application framework designed for rapid development of data science and analytical dashboards.
- In our project architecture:
  $$\text{Raw CSV (72,856)} \xrightarrow{\text{PySpark ETL}} \text{Cleaned Parquet (72,108)} \xrightarrow{\text{PySpark Agg}} \text{CSVs} \xrightarrow{\text{DataStax Driver}} \text{Cassandra (Serving Layer)} \xrightarrow{\text{CQL SELECT}} \text{Streamlit / Plotly}$$
- **Key Viva Distinction**:
  - The dashboard **does not connect directly to Apache Spark** during rendering. Spark is an offline/batch compute engine.
  - The dashboard connects **directly to Apache Cassandra** via port `9042`. Cassandra acts as the low-latency operational serving database, allowing the dashboard to fetch pre-aggregated metrics in single-digit milliseconds without launching heavy Spark distributed tasks.

---

### What is the Dual Data Architecture (Cassandra Serving + In-Memory Filter Engine)?
1. **Canonical Serving Layer (Apache Cassandra 4.1)**:
   - Stores pre-aggregated benchmark metrics:
     - `overall_metrics` (1 row)
     - `hourly_demand` (24 rows, 00:00 to 23:00)
     - `charger_type_metrics` (2 rows: AC, DC)
     - `location_metrics` (10 rows: facility categories)
   - Used for the full portfolio overview and baseline metrics.
2. **Interactive Slice Engine (Cleaned Parquet Cache)**:
   - Contains 72,108 validated session records (`Demand`, `Duration`, `charger_current_type`, `Location`, `start_hour`).
   - Cached in memory using `@st.cache_data(ttl=300)` (occupies only ~8.0 MB in RAM).
   - Allows instant, multi-dimensional cross-filtering across any combination of:
     - Charger Architecture: All, AC, DC
     - Facility Siting Category: All or 10 specific categories
     - Session Commencement Hour Window: 00:00 to 23:00 (slider or presets)

### Why is this dual approach architecturally sound and honest?
- **Viva Examiner Question**: *"Why don't you do multi-dimensional filtering directly in Cassandra?"*
- **Examiner Answer**:
  - In Cassandra, data modeling is **query-driven**. The four core tables are pre-aggregated single-dimensional projections:
    - `hourly_demand` is grouped by `start_hour` only (no charger type or location column).
    - `charger_type_metrics` is grouped by `charger_current_type` only.
    - `location_metrics` is grouped by `location` only.
  - If a user asks: *"Show me DC fast charging at apartment complexes between 16:00 and 20:00"*, Cassandra's pre-aggregated tables do not have the 3-way intersection columns.
  - Rather than creating an overly complex new database schema or falsely claiming that aggregate tables support 3-way cross-filtering, the dashboard honestly and efficiently reads from the cleaned session-level Parquet cache in memory.
  - The dashboard explicitly labels the source:
    - Default state: `Source: Apache Cassandra 4.1 Serving Layer (Canonical Benchmark Aggregates)`
    - Filtered slice: `Source: In-Memory Multi-Dimensional Filter Engine (Cleaned Parquet Cache)`

---

## 2. Interactive Filtering & Mathematical Integrity

### What filters are implemented?
1. **Charger Architecture**: All, AC (Level 2 / Slow), DC (Fast / Rapid).
2. **Facility / Siting Category**: All Facilities or 10 specific categories (`apartment`, `public area`, `hotel`, `resort`, `company`, etc.).
3. **Session Commencement Hour**: 00:00 to 23:00 slider with quick presets (`All 24h`, `Peak (16-20h)`, `Night (22-06h)`).
4. **Metric Selector (Tab 2)**: Total Energy (kWh), Session Count, Average Energy (kWh), Average Duration (min).
5. **Location Sorting Selector (Tab 4)**: Rank facilities by Volume, Total Energy, Average Demand, or Average Dwell.

### How are KPIs and averages calculated? (Mathematical Rigor)
- **Total Sessions ($N$)**:
  $$N = \sum_{i=1}^M 1$$
- **Total Delivered Energy ($E$)**:
  $$E = \sum_{i=1}^N \text{Demand}_i \quad (\text{kWh})$$
- **Average Energy Demand per Session ($\bar{E}$)**:
  $$\bar{E} = \frac{E}{N} = \frac{\sum_{i=1}^N \text{Demand}_i}{N} \quad (\text{kWh/session})$$
  *(Calculated strictly as sum of energy divided by session count. We never compute an unweighted average of averages, avoiding Simpson's paradox).*
- **Average Session Duration ($\bar{D}$)**:
  $$\bar{D} = \frac{\sum_{i=1}^N \text{Duration}_i}{N} \quad (\text{minutes})$$
- **AC / DC Volume Distribution**:
  $$\text{Share}_{\text{AC}} = \frac{N_{\text{AC}}}{N} \times 100\%, \quad \text{Share}_{\text{DC}} = \frac{N_{\text{DC}}}{N} \times 100\%$$
- **Active Slice Share of Full Portfolio**:
  $$\text{Portfolio Share} = \frac{N_{\text{slice}}}{72,108} \times 100\%$$
- **Empty Filter Handling**:
  - If a filter combination has 0 matching records (e.g., DC chargers at camping sites at 03:00), the dashboard displays a clear empty-state callout rather than misleading zero-division errors or broken charts.

---

## 3. Explaining Dashboard Visualizations in a Viva

### Tab 1: Overview & KPIs
- **4 Primary Metric Cards**:
  - Total Sessions, Total Energy (with GWh/MWh scaling), Average Demand (kWh), and Average Dwell (Hours & Minutes, plus min/max observed).
- **24-Hour Energy Demand Profile**:
  - Continuous spline curve showing delivered kWh by session start hour.
  - Automatically flags the peak demand hour (`18:00` with `118,980 kWh` in default portfolio).
- **Session Commencement Frequency**:
  - Bar chart showing transaction counts across 24 hours.
- **Dynamic Insight Cards**:
  - Busiest start window, off-peak night valley, AC vs DC dwell ratio, and primary siting category.

### Tab 2: Hourly Demand Profiling
- **Metric Selector**: Toggle between Total kWh, Session Count, Avg Demand, or Avg Dwell across 24h.
- **Dynamic Peak & Minimum Callouts**: Derived dynamically using `get_metric_peak_valley()`.
- **Complete 24-Hour Dataset Table**: Tabular breakdown with formatting.
- **Methodological Note for Examiners**:
  - *Start Hour vs. Instantaneous Power*: The hourly metrics aggregate transactions by the time the vehicle **plugged in** (commenced charging). This models driver arrival behavior, not instantaneous sub-station power flow.

### Tab 3: Charger Comparison (AC vs DC)
- **AC Level 2 (Destination)**:
  - Accounts for **79.6% (57,398 sessions)** of total volume.
  - Mean duration is **182.5 minutes (~3.0 hours)**.
  - Ideal for *destination charging* (residential apartments, office workplaces, hotels).
- **DC Fast Charging (Transit)**:
  - Accounts for **20.4% (14,710 sessions)** of volume.
  - Mean duration is **36.6 minutes** (delivering 19.2 kWh).
  - **5.0x faster turnaround** than AC, designed for *en-route highway corridors* and quick turnaround.

### Tab 4: Location & Siting Analysis
- Ranks 10 facility categories (`public area`, `apartment`, `resort`, `hotel`, `company`, etc.).
- Sortable by volume, energy, avg demand, or avg dwell.
- **Top 2 Categories**: `public area` (13,952 sessions) and `apartment` (13,913 sessions) capture over 38% of all charging volume.
- **Urban Planning Finding**: Overnight residential Level 2 charging and public parking hubs yield maximal infrastructure utilization.

### Tab 5: Architecture & Provenance
- Flow diagram of the end-to-end Big Data pipeline.
- Cassandra CQL schema specifications.
- Verified cleaning audit: 72,856 raw records $\to$ 72,108 clean records (748 anomalous records removed).
- Documented limitations (no GPS coordinates, no battery SoC, start-hour vs instantaneous power flow).

---

## 4. Summary Table for Viva Examiners

| Component | Technology | Primary Source | Query / Processing Frequency |
| :--- | :--- | :--- | :--- |
| **Portfolio KPIs** | Streamlit Metric Containers | `overall_metrics` (Cassandra) / Parquet slice | Cached (300s TTL) |
| **24h Load Curve** | Plotly Spline / Area Line | `hourly_demand` (Cassandra) / Parquet slice | Cached (300s TTL) |
| **Session Frequency** | Plotly Colored Bar Chart | `hourly_demand` (Cassandra) / Parquet slice | Cached (300s TTL) |
| **AC vs DC Comparison** | Plotly Grouped Bar Chart | `charger_type_metrics` / Parquet slice | Cached (300s TTL) |
| **Location Leaderboard** | Plotly Horizontal Bar Chart | `location_metrics` / Parquet slice | Cached (300s TTL) |
| **Dynamic Findings** | Pure Python Calculation | Derived from active slice | Computed on filter change |

---

## 5. Frequently Asked Viva Questions

**Q1: Why did you use Cassandra instead of MySQL or PostgreSQL?**  
*Answer:* Cassandra is a distributed NoSQL column-family database optimized for high-throughput write scalability and low-latency reads via partition keys. In a nationwide EV charging network streaming thousands of charging updates per minute, Cassandra scales horizontally across clusters with no single point of failure.

**Q2: Why not run queries directly on Spark from the dashboard?**  
*Answer:* Apache Spark is an offline, batch distributed compute engine. Launching a SparkContext or running a Spark job for every UI click would take 5 to 15 seconds of JVM execution time. Serving pre-aggregated summaries from Apache Cassandra takes under 15 milliseconds.

**Q3: Why is there no geographic map?**  
*Answer:* The source dataset (*Nature Scientific Data*, 2024 / IEEE PES Task Force) intentionally omits exact latitude and longitude coordinates to preserve EV user privacy. It provides categorical facility classifications (`apartment`, `hotel`, etc.) instead. Fabricating coordinates would compromise research integrity.

**Q4: How does caching improve user experience?**  
*Answer:* Streamlit reruns the Python script on every user interaction. `@st.cache_data(ttl=300)` caches the Cassandra query results and cleaned Parquet in memory, allowing interactive widget changes (e.g. changing the slider) to update instantly without network roundtrips to Cassandra. The `🔄 Clear Cache & Refresh` button allows forced invalidation.
