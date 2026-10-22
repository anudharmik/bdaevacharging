# Electric Vehicle (EV) Charging Station Analytics

[![Big Data Lab](https://img.shields.io/badge/Academic%20Project-Big%20Data%20Analytics%20Lab-blue.svg)](#)
[![Apache Spark](https://img.shields.io/badge/Engine-Apache%20Spark%203.5.1-E25A1C.svg?logo=apachespark)](#)
[![Apache Cassandra](https://img.shields.io/badge/Database-Apache%20Cassandra%204.1-1287B1.svg?logo=apachecassandra)](#)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB.svg?logo=python)](#)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B.svg?logo=streamlit)](#)

A complete Big Data Analysis laboratory project demonstrating end-to-end distributed data processing, NoSQL persistence, and interactive business intelligence on real-world electric vehicle charging transactions.

---

## 1. Project Objective

The primary objective of this project is to analyze charging behavior, station utilization, and power grid demand patterns using distributed computing and scalable NoSQL database storage.

- **Data Pipeline**: Ingestion and transformation of 72,856 real-world EV charging transaction sessions.
- **Distributed Computing**: Apache Spark (PySpark) for distributed data cleaning, timestamp parsing, outlier filtering, and analytical metric aggregation.
- **NoSQL Persistence**: Apache Cassandra for query-driven time-series and aggregated metric storage, leveraging partition keys for constant-time lookups.
- **Interactive Visualization**: Streamlit and Plotly dashboard providing interactive station utilization, hourly grid load heatmaps, and charger efficiency reports.

---

## 2. Architecture

```mermaid
flowchart LR
    subgraph Ingestion
        RAW["data/raw/ChargingRecords.csv<br/>(72,856 Sessions)"]
    end

    subgraph "Distributed Processing (Apache Spark)"
        SPARK["PySpark DataFrames<br/>Cleaning & Aggregation"]
    end

    subgraph "Storage & Serving (NoSQL)"
        CASSANDRA[("Apache Cassandra 4.1<br/>Port: 9042<br/>Keyspace: ev_analytics")]
    end

    subgraph "Presentation Layer"
        DASH["Streamlit Dashboard<br/>+ Plotly Visualizations<br/>Port: 8501"]
    end

    RAW --> SPARK
    SPARK -->|Spark Cassandra Connector / Driver| CASSANDRA
    CASSANDRA -->|Query CQL by Partition Keys| DASH
```

### High-Level Data Flow:
1. **Raw Ingestion**: Real-world transaction logs collected over one annual commercial operating period.
2. **Distributed Processing**: PySpark parses timestamps, cleans anomalous durations, calculates session kilowatt throughput, and computes aggregations (hourly grid load, AC vs. DC charger utilization, location efficiency).
3. **Optimized Cassandra Modeling**: Cassandra tables designed around specific query access patterns (partitioned by charger, date, and location) rather than relational normalization.
4. **Interactive Dashboard**: Streamlit queries pre-aggregated analytical tables from Cassandra to deliver sub-second visual responses.

---

## 3. Project Directory Structure

```text
.
├── cassandra/
│   └── init-scripts/          # CQL schema initialization files
├── data/
│   ├── raw/                   # Raw immutable datasets (e.g., ChargingRecords.csv)
│   └── processed/             # Cleaned Parquet / intermediary data
├── docs/
│   └── dataset_spec.md        # Comprehensive dataset documentation and schema
├── scripts/
│   ├── check_env.py           # Environment, runtime, and directory verification script
│   └── check_cassandra.py     # Cassandra TCP and CQL connectivity verification script
├── src/
│   ├── analytics/             # PySpark transformation and aggregation jobs
│   ├── database/              # Cassandra connection management and CQL data access layer
│   └── dashboard/             # Streamlit visual application components
├── tests/
│   └── test_scaffold.py       # Unit tests verifying project structure and configuration
├── .env.example               # Configuration template (copy to .env for local adjustments)
├── .gitignore                 # Rules for excluding secrets, caches, and datasets
├── docker-compose.yml         # Containerized single-node Apache Cassandra 4.1 service
├── README.md                  # Project documentation and guide
└── requirements.txt           # Pinned, tested Python dependency specification
```

---

## 4. Prerequisites

| Tool | Recommended Version | Verification Command | Notes |
| :--- | :--- | :--- | :--- |
| **Python** | 3.11.x | `python --version` or `py -3.11 --version` | Required for binary compatibility with PySpark and compiled wheels. |
| **Java (JDK)** | Java 8, 11, or 17 | `java -version` | Required by PySpark. Ensure `JAVA_HOME` is set. |
| **Docker** | 20.10+ | `docker --version` | Docker Desktop running with Linux containers (WSL2 backend on Windows). |
| **Docker Compose** | v2.0+ | `docker compose version` | Standard Compose CLI plugin. |

---

## 5. Setup & Getting Started

### Step 1: Clone and Configure Environment
1. Clone this repository or open the project folder in your terminal.
2. Create local configuration file from the template:
   ```powershell
   Copy-Item .env.example .env
   ```

### Step 2: Set Up Python Virtual Environment
We strongly recommend creating a virtual environment with **Python 3.11**:
```powershell
# Create virtual environment
py -3.11 -m venv .venv

# Activate virtual environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Upgrade pip
python -m pip install --upgrade pip

# Install pinned dependencies
pip install -r requirements.txt
```

### Step 3: Start Apache Cassandra Service
Start the single-node Cassandra instance in the background using Docker Compose:
```powershell
# Start container
docker compose up -d

# Check status and healthcheck progress
docker compose ps
```
> **Note**: Cassandra performs JVM memory initialization and gossip ring bootstrap. The container healthcheck will transition from `(health: starting)` to `(healthy)` within ~30 to 45 seconds.

### Step 4: Run Verification Checks
Run the automated environment and connectivity check scripts:
```powershell
# Verify Python, Java, Docker, and directories
python scripts/check_env.py

# Verify Cassandra TCP & CQL connectivity
python scripts/check_cassandra.py

# Run unit tests
pytest tests/
```

---

## 6. Dataset Plan

- **Dataset**: *A dataset for multi-faceted analysis of electric vehicle charging transactions*
- **Publisher**: IEEE Task Force on Data Sharing in Energy Systems / *Nature Scientific Data* (2024).
- **Scale**: **72,856 genuine sessions**, 2,337 users, 2,119 chargers.
- **License**: Creative Commons Attribution 4.0 International (CC BY 4.0).
- **File Name**: `ChargingRecords.csv` (~7.6 MB).
- **Documentation**: See [`docs/dataset_spec.md`](docs/dataset_spec.md) for the 16-column schema and download instructions.

To download the dataset into the project directory:
```powershell
curl.exe -L "https://ndownloader.figshare.com/files/39952252" -o "data/raw/ChargingRecords.csv"
```

---

## 7. Implementation Roadmap

- [x] **Phase 1: Environment & Project Scaffold**
  - Project directory hierarchy.
  - Containerized single-node Cassandra with persistent volume and healthcheck.
  - Dependency pinning (`pyspark==3.5.1`, `cassandra-driver==3.29.2`, `streamlit`, `plotly`).
  - Environment and Cassandra connection verification scripts.
  - Dataset research, schema specification, and download procedures.
- [ ] **Phase 2: Data Ingestion & PySpark Processing Pipeline**
  - PySpark DataFrame schema definition and CSV loader.
  - Data cleaning: NULL handling, date/timestamp parsing, duration validation.
  - Feature engineering: charging speed classification, peak-hour tags.
  - Aggregation jobs: Hourly demand, charger utilization, location trends.
- [ ] **Phase 3: Apache Cassandra Data Modeling & Loading**
  - Design query-specific CQL schema (keyspaces, partition keys, clustering columns).
  - Bulk insertion pipeline writing PySpark results into Cassandra tables.
  - Unit and integration tests validating CQL queries.
- [ ] **Phase 4: Streamlit & Plotly Analytical Dashboard**
  - Streamlit application structure and Cassandra data access layer.
  - Interactive filters: Date range, charger type (AC/DC), location categories.
  - Plotly visualizations: Grid load profile, utilization heatmaps, station performance KPIs.
- [ ] **Phase 5: Testing, Documentation & Viva Preparation**
  - End-to-end integration test.
  - Performance considerations and architecture presentation slides/notes.
