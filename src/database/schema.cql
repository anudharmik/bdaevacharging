-- ==============================================================================
-- schema.cql
-- Analytical tables for EV Charging Station Analytics
-- ==============================================================================

CREATE KEYSPACE IF NOT EXISTS ev_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
} AND durable_writes = true;

USE ev_analytics;

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

CREATE TABLE IF NOT EXISTS hourly_demand (
    start_hour int,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (start_hour)
);

CREATE TABLE IF NOT EXISTS charger_type_metrics (
    charger_current_type text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (charger_current_type)
);

CREATE TABLE IF NOT EXISTS location_metrics (
    location text,
    session_count bigint,
    total_demand_kwh double,
    avg_demand_kwh double,
    avg_duration_minutes double,
    PRIMARY KEY (location)
);
