# Viva Examination Notes & Technical Guide: Interactive Dashboard

This document provides clear, viva-ready explanations of the presentation tier, Streamlit framework, Plotly visualizations, data caching mechanisms, and how the web dashboard interfaces with Apache Cassandra.

---

## 1. Presentation Tier Architecture

### What is the role of Streamlit in this Big Data project?
- **Streamlit** is a Python-native web application framework designed for rapid development of data science and analytical dashboards.
- In our project architecture:
  $$\text{Raw CSV} \xrightarrow{\text{PySpark}} \text{Analytics CSVs} \xrightarrow{\text{DataStax Driver}} \text{Apache Cassandra} \xrightarrow{\text{CQL SELECT}} \text{Streamlit / Plotly}$$
- **Key Viva Distinction**:
  - The dashboard **does not connect to Apache Spark**. Spark is an offline/batch compute engine.
  - The dashboard connects **directly to Apache Cassandra** via port `9042`. Cassandra acts as the low-latency operational serving database, allowing the dashboard to fetch pre-aggregated metrics in single-digit milliseconds without launching heavy Spark distributed tasks.

---

### What is Plotly and why is it used over Matplotlib or Seaborn?
- **Matplotlib & Seaborn**: Produce static raster images (PNG/JPEG). Users cannot zoom, hover over data points to inspect exact values, or filter series dynamically.
- **Plotly**: Generates client-side **interactive JavaScript visualizations** (WebGL/D3.js). When hovering over the 24-hour load curve, users see exact timestamps and demand figures (`118,980 kWh at 18:00`). Plotly charts are fully responsive and fit desktop or mobile displays cleanly.

---

## 2. Cassandra Data Access & Caching Strategy

### How does the dashboard retrieve data from Cassandra?
The dashboard uses the DataStax Python driver (`cassandra-driver==3.29.2`) inside `load_data_from_cassandra()`:
```python
cluster, session = get_cassandra_session()
overall = fetch_overall_metrics(session)      # SELECT FROM overall_metrics WHERE metric_id = 'overall'
hourly = fetch_hourly_demand(session)         # SELECT FROM hourly_demand (24 rows)
charger_types = fetch_charger_type_metrics(session)  # SELECT FROM charger_type_metrics (2 rows)
locations = fetch_location_metrics(session)   # SELECT FROM location_metrics (10 rows)
```
Total records transferred across the network: **37 rows**.
Query execution time: **< 15 milliseconds**.

---

### Why is `@st.cache_data(ttl=60)` used?
- Streamlit reruns the entire Python script from top to bottom on every user interaction (e.g., clicking a button, sorting a select box).
- Without caching, every user click would execute 4 network queries to the Cassandra cluster.
- `@st.cache_data(ttl=60)` caches the query result in memory for 60 seconds:
  - If a user changes a dropdown or re-sorts a chart within 60 seconds, the dashboard renders instantly from RAM.
  - The `🔄 Refresh Data` button calls `st.cache_data.clear()` to force an immediate reload from Cassandra.

---

### How is database reliability handled?
1. **No Silent Fallbacks**: If the Cassandra container is stopped, the application displays a prominent error banner with troubleshooting commands (`docker compose up -d`). It never secretly falls back to CSV files, ensuring integrity.
2. **Empty Table Detection**: If Cassandra is reachable but tables are empty, it alerts the user to run `python scripts/load_cassandra.py`.

---

## 3. Explaining Dashboard Visualizations in a Viva

### 1. KPI Cards (Top Banner)
- **Total Sessions (72,108)**: Number of verified commercial transactions after conservative cleaning (dropping 13 duplicates and 735 non-positive duration anomalies).
- **Total Energy (1.27 GWh / 1,267,066 kWh)**: Scale of electrical energy delivered by the charging network over 12 months.
- **Average Energy / Session (17.6 kWh)**: Typical battery top-up amount for passenger EVs.
- **Average Duration (152.7 min / ~2h 33m)**: Mean connected dwell time across all charger locations.

---

### 2. 24-Hour Grid Demand Curve (Hourly Profile)
- **The Graph**: Line chart showing total kWh consumption from hour `00:00` to `23:00`.
- **Engineering Finding**:
  - **Peak Hour**: Occurs at **18:00 (6:00 PM)** with **6,435 sessions** consuming **118,980 kWh**.
  - **Coincidence Factor**: This peak coincides directly with peak domestic residential energy consumption (lighting, cooking, HVAC).
  - **Grid Solution**: Utility companies use this finding to implement time-of-use (TOU) tariffs—charging higher electricity rates at 18:00 to incentivize drivers to delay charging until off-peak hours (e.g., 03:00 valley).

---

### 3. AC Level 2 vs. DC Fast Charger Comparison
- **AC Chargers**:
  - Account for **79.6% (57,398 sessions)** of total charging.
  - Average duration is **182.5 minutes (~3.0 hours)**.
  - Ideal for *destination charging* (apartments, hotels, offices) where cars sit parked for extended periods.
- **DC Fast Chargers**:
  - Account for **20.4% (14,710 sessions)**.
  - Average duration is only **36.6 minutes** (delivering 19.2 kWh).
  - **5.0x faster throughput** than AC, designed for *en-route highway charging* and quick vehicle turnaround.

---

### 4. Siting & Location Categories
- Evaluates 10 facility categories (`public area`, `apartment`, `resort`, `hotel`, `company`, etc.).
- **Top 2 Categories**: `public area` (13,952 sessions) and `apartment` (13,913 sessions) generate over 38% of all charging volume.
- **Urban Planning Finding**: Siting EV chargers in residential apartment complexes and public parking areas maximizes infrastructure utilization.

---

## 4. Summary Table for Viva Examiners

| Component | Technology | Source Table | Query Frequency |
| :--- | :--- | :--- | :--- |
| **KPIs** | Streamlit Metric Containers | `overall_metrics` | Cached (60s TTL) |
| **24h Load Curve** | Plotly Scatter / Area Line | `hourly_demand` | Cached (60s TTL) |
| **Session Frequency** | Plotly Express Bar Chart | `hourly_demand` | Cached (60s TTL) |
| **AC vs DC Split** | Plotly Categorical Bar Chart | `charger_type_metrics` | Cached (60s TTL) |
| **Location Ranking** | Plotly Horizontal Bar Chart | `location_metrics` | Cached (60s TTL) |
| **Dynamic Findings** | Pure Python Calculations | Derived from query results | Computed per refresh |
