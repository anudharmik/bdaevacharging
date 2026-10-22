-- ==============================================================================
-- 01_create_keyspace.cql
-- Keyspace Definition for EV Charging Station Analytics
-- Single-node development cluster uses SimpleStrategy with replication_factor 1
-- ==============================================================================

CREATE KEYSPACE IF NOT EXISTS ev_analytics
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
} AND durable_writes = true;
