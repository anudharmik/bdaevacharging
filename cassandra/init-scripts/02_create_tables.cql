-- ==============================================================================
-- 02_create_tables.cql
-- Analytical tables for EV Charging Station Analytics in Apache Cassandra
-- Keyspace: ev_analytics | Target Port: 9042
-- ==============================================================================

USE ev_analytics;

-- Table 1: Overall portfolio summary (metric_id = 'overall')
-- Powers executive KPI cards in Tab 1
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

-- Table 2: Hourly energy demand profile (start_hour = 0..23)
-- Powers 24h demand spline curve, frequency bar chart, and Tab 2 deep-dive
CREATE TABLE IF NOT EXISTS hourly_demand (
    start_hour int,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (start_hour)
);

-- Table 3: Charger type utilization and throughput (charger_current_type = 'AC' | 'DC')
-- Powers Tab 3 Charger Comparison table, volume/energy charts, and speedup ratios
CREATE TABLE IF NOT EXISTS charger_type_metrics (
    charger_current_type text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (charger_current_type)
);

-- Table 4: Location category metrics (location = 'apartment', 'public area', etc.)
-- Powers Tab 4 Siting Leaderboard and horizontal ranking bar charts
CREATE TABLE IF NOT EXISTS location_metrics (
    location text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (location)
);
