-- ==============================================================================
-- 02_create_tables.cql
-- Analytical tables for EV Charging Station Analytics
-- Designed for query-driven lookups without joins or full-table scans
-- ==============================================================================

USE ev_analytics;

-- Table 1: Overall portfolio summary
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

-- Table 2: Hourly energy demand profile (0 to 23 hours)
CREATE TABLE IF NOT EXISTS hourly_demand (
    start_hour int,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (start_hour)
);

-- Table 3: Charger type utilization and throughput (AC vs DC)
CREATE TABLE IF NOT EXISTS charger_type_metrics (
    charger_current_type text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (charger_current_type)
);

-- Table 4: Location category metrics
CREATE TABLE IF NOT EXISTS location_metrics (
    location text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (location)
);
