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
- **21 passed tests** covering:
  - Project directory structure & config validation
  - In-memory PySpark DataFrame ETL transformations
  - Cassandra offline data mapping & type conversions
  - Dashboard formatting & dynamic insight calculators

---

### Step 4: Launch the Interactive Streamlit Dashboard
```powershell
.\.venv\Scripts\streamlit.exe run src/dashboard/app.py --server.port 8501
```
Open `http://localhost:8501` in your browser. Point out the top-right status badge: `🟢 Connected to Cassandra | Keyspace: ev_analytics | Port: 9042`.

---

### Step 5: Explain the 4 KPI Cards
Point to the executive metric cards:
1. **Total Charging Sessions:** `72,108` sessions (verified commercial transactions).
2. **Total Energy Delivered:** `1.27 GWh (1,267,066 kWh)`.
3. **Average Energy / Session:** `17.6 kWh` (typical passenger vehicle charge amount).
4. **Average Dwell Time:** `2h 33m (152.7 min)`.

---

### Step 6: Demonstrate 24-Hour Energy Demand Profile
Hover over the Plotly line chart and bar chart:
- Point out the red callout marker: **Peak: 118,980 kWh at 18:00**.
- Explain: Sessions are grouped by **session start hour**. The 18:00 (6:00 PM) evening surge corresponds to post-commute plug-ins.
- Point out the night valley at **03:00 (133 sessions)** as prime capacity for off-peak valley-filling tariffs.

---

### Step 7: Compare AC vs. DC Charger Architecture
Click between the two tabs under *AC vs. DC Charger Architecture*:
- **Tab 1 (Session & Energy Split):** AC Level-2 represents **79.6%** of sessions, serving destination charging.
- **Tab 2 (Dwell Time):** Show the duration difference—DC fast chargers average **36.6 minutes**, demonstrating a **5.0x lower average session duration** compared to AC (182.5 min).

---

### Step 8: Demonstrate Interactive Location Sorting
Scroll to *Siting & Location Categories*:
- Use the dropdown `Sort Locations By:` to toggle between:
  1. *Session Count (Volume)*
  2. *Total Energy Delivery (kWh)*
  3. *Average Duration (Minutes)*
- Explain that `public area` (13,952 sessions) and `apartment` complexes (13,913 sessions) dominate total demand, informing commercial charger placement.

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
