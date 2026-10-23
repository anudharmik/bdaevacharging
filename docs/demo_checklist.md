# Viva Demonstration Checklist & Step-by-Step Guide

**Project:** Electric Vehicle Charging Station Analytics using Apache Spark and Cassandra  
**Time Required for Demo:** ~5–7 Minutes  
**Target Audience:** Lab Evaluator / Viva Examiner  

---

## Pre-Demo Checklist

- [ ] Open PowerShell in repository root: `E:\BDA ev charge`
- [ ] Confirm Python 3.11 virtual environment exists: `.\.venv\Scripts\Activate.ps1`
- [ ] Ensure Docker Desktop is running

---

## 10-Step Live Demonstration Procedure

### Step 1: Start & Verify Apache Cassandra Container
Show the evaluator that Cassandra is running inside Docker with proper health checks and constrained memory limits.
```powershell
docker compose ps
```
*Expected Output:* Container `ev_cassandra` has status `Up (healthy)` on port `9042`.

---

### Step 2: Check Cassandra Stored Tables & Data Parity
Run the verification script to prove live CQL data retrieval from Cassandra.
```powershell
.\.venv\Scripts\python.exe scripts/check_cassandra_data.py
```
*What to highlight to examiner:*
- Show that all 4 tables exist and match source metrics:
  - `overall_metrics`: 1 row (72,108 sessions, 1.27 GWh)
  - `hourly_demand`: 24 rows (hours 0 to 23)
  - `charger_type_metrics`: 2 rows (AC and DC)
  - `location_metrics`: 10 rows

---

### Step 3: Run the Complete Unit Test Suite
Demonstrate automated testing across all pipeline components.
```powershell
.\.venv\Scripts\pytest.exe tests/
```
*What to highlight to examiner:*
- **32 passed tests** covering:
  - Project directory structure & config validation
  - In-memory PySpark DataFrame ETL transformations
  - Cassandra offline data mapping & type conversions
  - Multi-dimensional filtering, weighted average calculation, duration bounds, and dynamic peak/valley calculators

---

### Step 4: Launch the Interactive Streamlit Dashboard
```powershell
.\.venv\Scripts\streamlit.exe run src/dashboard/app.py --server.port 8501
```
Open `http://localhost:8501` in your browser. Point out the top-right status badge: `🟢 Connected to Cassandra | Serving Keyspace: ev_analytics | Port: 9042`.

---

### Step 5: Explain the 4 Primary KPI Cards & Dual Data Architecture
Point to the executive metric cards:
1. **Total Charging Sessions:** `72,108` sessions (verified commercial transactions).
2. **Total Energy Delivered:** `1.27 GWh (1,267,066 kWh)`.
3. **Average Energy / Session:** `17.57 kWh` (typical passenger vehicle charge amount).
4. **Average Dwell Time:** `2h 33m (152.7 min)` (observed range: 1 min to 26.2 hours).

*Highlight the Dual Architecture*:
- Default view is served with sub-second latency from **Apache Cassandra 4.1**.
- Interactive cross-filtering runs via an in-memory analytical cache of the 72,108 cleaned records from Parquet.

---

### Step 6: Demonstrate Interactive Multi-Dimensional Filtering
Show the evaluator how changing the sidebar controls dynamically updates all numbers, charts, and insights:
1. **Filter by Charger Type:** Select `DC (Fast / Rapid)`:
   - Notice sessions drop to **14,710** (20.4% of portfolio).
   - Average dwell immediately updates to **36.6 minutes**.
2. **Filter by Facility Category:** Select `Apartment`:
   - Shows residential charging profile.
3. **Filter by Start Hour Window:** Drag slider to `16:00 – 20:00`:
   - Highlights evening peak load concentration.
4. **Reset Filters:** Click `↺ Clear Cache & Refresh` or select `All` to restore the full portfolio view.

---

### Step 7: Explore the 5 Operational Tabs
1. **📊 Overview & KPIs:** 24h demand profile spline and arrival frequency bar chart with peak callouts.
2. **⚡ Hourly Demand Profiling:** Metric selector toggling between Total kWh, Sessions, Avg Demand, and Avg Dwell, with dynamic peak & valley cards.
3. **🔌 Charger Comparison:** Head-to-head table showing DC chargers achieve **5.0x faster turnaround** (36.6 min vs. 182.5 min for AC).
4. **🏢 Location & Siting Analysis:** Siting leaderboard sortable by volume, energy, avg demand, or avg dwell across all 10 categories.
5. **ℹ️ Architecture & Provenance:** End-to-end pipeline flow diagram, Cassandra schema reference, and dataset provenance audit.

---

### Step 9: Walk Through Data-Driven Findings & Architectural Separation
Highlight the 4 dynamic insight callout containers at the bottom.
Explain the pipeline architecture:
$$\text{Raw CSV} \xrightarrow{\text{PySpark Cleaning \& Aggregation}} \text{Analytics CSV} \xrightarrow{\text{DataStax Driver}} \text{Cassandra} \xrightarrow{\text{Streamlit Dashboard}}$$
- Reiterate that the dashboard **does not run Spark jobs on user clicks**; Cassandra serves the queries in under 15 milliseconds.

---

### Step 10: State Known Dataset Limitations Accurately
Conclude by presenting academic honesty regarding limitations:
1. **No GPS Coordinates:** The dataset omits latitude/longitude for user privacy; facility classifications were analyzed instead of generating fake map pins.
2. **Plugged Time vs. Current Flow:** Duration represents physical plug-in time; vehicles parked overnight in apartments may finish charging earlier.

---

## Clean Shutdown Procedure

When the demonstration concludes:
1. In the PowerShell terminal running Streamlit, press `Ctrl + C` to stop the web server.
2. To stop the Cassandra container:
   ```powershell
   docker compose down
   ```
*(Note: Data is preserved in the persistent volume `ev_cassandra_data` for subsequent runs).*

---

## Troubleshooting Guide

| Issue | Cause | Solution |
| :--- | :--- | :--- |
| **"Failed to connect to Apache Cassandra"** | Docker container is stopped or still starting. | Run `docker compose up -d` and wait ~30 seconds for health check (`docker compose ps`). |
| **"Tables are empty in keyspace ev_analytics"** | Schema created but loader hasn't run. | Run `.\.venv\Scripts\python.exe scripts/load_cassandra.py`. |
| **Port 8501 in use** | Another Streamlit instance is active. | Launch on another port: `streamlit run src/dashboard/app.py --server.port 8502`. |
| **`python` uses Python 3.14 instead of 3.11** | System PATH points to Python 3.14. | Always use `.\.venv\Scripts\python.exe` explicitly. |
