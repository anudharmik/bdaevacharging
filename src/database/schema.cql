-- ==============================================================================
-- schema.cql
-- Analytical Tables for EV Charging Station Analytics in Apache Cassandra
-- Keyspace: ev_analytics | Target Port: 9042
--
-- Query-Driven Data Modeling:
-- In Cassandra, relational JOIN operations across distributed nodes are prohibitively
-- expensive. Each table is designed to answer one specific dashboard analytical query
-- with O(1) point lookups on its partition key.
-- ==============================================================================

CREATE KEYSPACE IF NOT EXISTS ev_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
} AND durable_writes = true;

USE ev_analytics;

-- ------------------------------------------------------------------------------
-- Table 1: overall_metrics
-- Stores      : Executive portfolio summary across all validated sessions (1 row).
-- Primary Key : metric_id (Partition Key: 'overall')
-- Why Exists  : Eliminates repeated full-table scans to compute portfolio totals.
-- Dashboard   : Powers the 4 primary KPI cards in Tab 1 (Sessions, kWh, Avg Demand, Dwell).
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS overall_metrics (
    metric_id text,
    total_sessions bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    min_duration_minutes double,
    max_duration_minutes double,
    PRIMARY KEY (metric_id)
);

-- ------------------------------------------------------------------------------
-- Table 2: hourly_demand
-- Stores      : 24-hour demand profile partitioned by commencement hour (24 rows: 0..23).
-- Primary Key : start_hour (Partition Key: 0 to 23)
-- Why Exists  : Direct indexed lookups for 24 discrete hourly grid arrival windows.
-- Dashboard   : Powers 24h demand spline curve, frequency bar chart, and Tab 2 deep-dive.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hourly_demand (
    start_hour int,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (start_hour)
);

-- ------------------------------------------------------------------------------
-- Table 3: charger_type_metrics
-- Stores      : Utilization, throughput, and dwell stats across charger modes (2 rows: AC, DC).
-- Primary Key : charger_current_type (Partition Key: 'AC' or 'DC')
-- Why Exists  : Instant comparative lookup between destination and transit charging.
-- Dashboard   : Powers Tab 3 Charger Comparison table, volume/energy charts, and speedup ratios.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS charger_type_metrics (
    charger_current_type text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (charger_current_type)
);

-- ------------------------------------------------------------------------------
-- Table 4: location_metrics
-- Stores      : Aggregated metrics across 10 installation categories (10 rows).
-- Primary Key : location (Partition Key: facility name, e.g., 'apartment', 'public area')
-- Why Exists  : Instant access to facility siting metrics without scanning transaction logs.
-- Dashboard   : Powers Tab 4 Siting Leaderboard and horizontal ranking bar charts.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS location_metrics (
    location text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (location)
);
