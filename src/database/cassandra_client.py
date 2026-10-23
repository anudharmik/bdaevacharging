"""
Apache Cassandra Client Layer for EV Charging Station Analytics.
Provides straightforward, explicit CQL functions using the official DataStax Python driver.
Avoids ORMs, metaclasses, and generic abstractions for straightforward viva explainability.

Architecture:
- Keyspace: ev_analytics (SimpleStrategy, RF=1 for local development)
- Native Transport Port: 9042
- Tables:
    1. overall_metrics      -> Executive KPI summary (1 row, metric_id='overall')
    2. hourly_demand        -> 24-hour load profile (24 rows, start_hour=0..23)
    3. charger_type_metrics -> AC vs DC utilization (2 rows, charger_current_type='AC'|'DC')
    4. location_metrics     -> Siting leaderboard (10 rows, location=facility name)
"""

import os
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

# Load environment configuration if python-dotenv is available
try:
    from dotenv import load_dotenv

    env_file = Path(__file__).resolve().parent.parent.parent / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file)
    else:
        example_env = Path(__file__).resolve().parent.parent.parent / ".env.example"
        if example_env.exists():
            load_dotenv(dotenv_path=example_env)
except ImportError:
    pass

# Default connection parameters
DEFAULT_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
DEFAULT_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
DEFAULT_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "ev_analytics")
DEFAULT_USER = os.environ.get("CASSANDRA_USERNAME", "cassandra")
DEFAULT_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")


# =============================================================================
# 1. Connection Management & Lifecycle
# =============================================================================


def get_cassandra_session(
    host: Optional[str] = None,
    port: Optional[int] = None,
    keyspace: Optional[str] = DEFAULT_KEYSPACE,
) -> Tuple[Cluster, Session]:
    """
    Establishes connection to the Apache Cassandra cluster via native transport (port 9042).
    Returns: (cluster, session) tuple.
    """
    target_host = host or DEFAULT_HOST
    target_port = port or DEFAULT_PORT

    auth_provider = PlainTextAuthProvider(
        username=DEFAULT_USER,
        password=DEFAULT_PASSWORD,
    )

    cluster = Cluster(
        contact_points=[target_host],
        port=target_port,
        auth_provider=auth_provider,
        connect_timeout=10,
    )
    session = cluster.connect()

    if keyspace:
        session.set_keyspace(keyspace)

    return cluster, session


def is_cassandra_available(
    host: Optional[str] = None,
    port: Optional[int] = None,
    timeout_sec: float = 1.0,
) -> bool:
    """
    Performs a fast TCP socket check to verify if the Cassandra native transport port is open.
    Prevents prolonged connection hangs when Cassandra is not running (e.g. on Streamlit Cloud).
    """
    target_host = host or DEFAULT_HOST
    target_port = port or DEFAULT_PORT
    try:
        with socket.create_connection((target_host, target_port), timeout=timeout_sec):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def shutdown_cassandra(cluster: Optional[Cluster], session: Optional[Session]) -> None:
    """Closes session and cluster connection cleanly to free network sockets."""
    if session and not session.is_shutdown:
        session.shutdown()
    if cluster and not cluster.is_shutdown:
        cluster.shutdown()


# =============================================================================
# 2. Schema Management & Table Creation
# =============================================================================


EXPECTED_TABLE_COLUMNS: Dict[str, set] = {
    "overall_metrics": {
        "metric_id",
        "total_sessions",
        "total_demand_kwh",
        "avg_demand_kwh",
        "avg_duration_minutes",
        "min_duration_minutes",
        "max_duration_minutes",
    },
    "hourly_demand": {
        "start_hour",
        "session_count",
        "total_demand_kwh",
        "avg_demand_kwh",
        "avg_duration_minutes",
    },
    "charger_type_metrics": {
        "charger_current_type",
        "session_count",
        "total_demand_kwh",
        "avg_demand_kwh",
        "avg_duration_minutes",
    },
    "location_metrics": {
        "location",
        "session_count",
        "total_demand_kwh",
        "avg_demand_kwh",
        "avg_duration_minutes",
    },
}


def verify_schema_compatibility(session: Session, keyspace: str = DEFAULT_KEYSPACE) -> None:
    """
    Verifies that existing tables match the expected columns via system_schema.columns.
    Prevents silent data corruption or accidental overwrites.
    """
    query = "SELECT table_name, column_name FROM system_schema.columns WHERE keyspace_name = %s;"
    rows = session.execute(query, [keyspace])
    existing_tables: Dict[str, set] = {}
    for r in rows:
        existing_tables.setdefault(r.table_name, set()).add(r.column_name)

    for table_name, expected_cols in EXPECTED_TABLE_COLUMNS.items():
        if table_name in existing_tables:
            actual_cols = existing_tables[table_name]
            missing_cols = expected_cols - actual_cols
            if missing_cols:
                raise RuntimeError(
                    f"Incompatible schema detected in table '{keyspace}.{table_name}'. "
                    f"Missing expected columns: {missing_cols}. Refusing to proceed."
                )


def create_tables(session: Session) -> None:
    """
    Creates the keyspace and the four core analytical tables if they do not exist.
    Verifies schema compatibility before execution.
    """
    # 1. Create Keyspace
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS ev_analytics
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        AND durable_writes = true;
        """
    )
    session.set_keyspace("ev_analytics")

    # Verify compatibility
    verify_schema_compatibility(session, "ev_analytics")

    # 2. Table 1: Overall summary
    session.execute(
        """
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
        """
    )

    # 3. Table 2: Hourly demand profile
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS hourly_demand (
            start_hour int,
            session_count bigint,
            total_demand_kwh double,
            avg_demand_kwh double,
            avg_duration_minutes double,
            PRIMARY KEY (start_hour)
        );
        """
    )

    # 4. Table 3: Charger type metrics (AC vs DC)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS charger_type_metrics (
            charger_current_type text,
            session_count bigint,
            total_demand_kwh double,
            avg_demand_kwh double,
            avg_duration_minutes double,
            PRIMARY KEY (charger_current_type)
        );
        """
    )

    # 5. Table 4: Location category metrics
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS location_metrics (
            location text,
            session_count bigint,
            total_demand_kwh double,
            avg_demand_kwh double,
            avg_duration_minutes double,
            PRIMARY KEY (location)
        );
        """
    )


# =============================================================================
# 3. Idempotent Data Insertion (Parameterized Upserts)
# =============================================================================


def insert_overall_metrics(session: Session, data: Dict[str, Any]) -> None:
    """Inserts or updates the overall metrics row (metric_id='overall')."""
    statement = session.prepare(
        """
        INSERT INTO overall_metrics (
            metric_id, total_sessions, total_demand_kwh,
            avg_demand_kwh, avg_duration_minutes,
            min_duration_minutes, max_duration_minutes
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """
    )
    session.execute(
        statement,
        [
            str(data.get("metric_id", "overall")),
            int(data["total_sessions"]),
            float(data["total_demand_kwh"]),
            float(data["avg_demand_kwh"]),
            float(data["avg_duration_minutes"]),
            float(data.get("min_duration_minutes", 0.0)),
            float(data.get("max_duration_minutes", 0.0)),
        ],
    )


def insert_hourly_demand(session: Session, rows: List[Dict[str, Any]]) -> None:
    """Inserts or updates hourly demand profile rows (hours 0..23)."""
    statement = session.prepare(
        """
        INSERT INTO hourly_demand (
            start_hour, session_count, total_demand_kwh,
            avg_demand_kwh, avg_duration_minutes
        ) VALUES (?, ?, ?, ?, ?);
        """
    )
    for row in rows:
        session.execute(
            statement,
            [
                int(row["start_hour"]),
                int(row["session_count"]),
                float(row["total_demand_kwh"]),
                float(row["avg_demand_kwh"]),
                float(row["avg_duration_minutes"]),
            ],
        )


def insert_charger_type_metrics(session: Session, rows: List[Dict[str, Any]]) -> None:
    """Inserts or updates charger type metrics rows (AC, DC)."""
    statement = session.prepare(
        """
        INSERT INTO charger_type_metrics (
            charger_current_type, session_count, total_demand_kwh,
            avg_demand_kwh, avg_duration_minutes
        ) VALUES (?, ?, ?, ?, ?);
        """
    )
    for row in rows:
        session.execute(
            statement,
            [
                str(row["charger_current_type"]),
                int(row["session_count"]),
                float(row["total_demand_kwh"]),
                float(row["avg_demand_kwh"]),
                float(row["avg_duration_minutes"]),
            ],
        )


def insert_location_metrics(session: Session, rows: List[Dict[str, Any]]) -> None:
    """Inserts or updates facility location category metrics rows."""
    statement = session.prepare(
        """
        INSERT INTO location_metrics (
            location, session_count, total_demand_kwh,
            avg_demand_kwh, avg_duration_minutes
        ) VALUES (?, ?, ?, ?, ?);
        """
    )
    for row in rows:
        session.execute(
            statement,
            [
                str(row["location"]),
                int(row["session_count"]),
                float(row["total_demand_kwh"]),
                float(row["avg_demand_kwh"]),
                float(row["avg_duration_minutes"]),
            ],
        )


# =============================================================================
# 4. Explicit Data Retrieval Functions (Dashboard Query Layer)
# =============================================================================


def fetch_overall_metrics(session: Session) -> Optional[Dict[str, Any]]:
    """
    Retrieves the executive overall metrics row.
    Dashboard usage: Powers the 4 primary KPI cards in Tab 1.
    CQL: SELECT ... FROM overall_metrics WHERE metric_id = 'overall';
    """
    query = """
    SELECT metric_id, total_sessions, total_demand_kwh, avg_demand_kwh,
           avg_duration_minutes, min_duration_minutes, max_duration_minutes
    FROM overall_metrics WHERE metric_id = 'overall';
    """
    row = session.execute(query).one()
    if not row:
        return None
    return {
        "metric_id": row.metric_id,
        "total_sessions": row.total_sessions,
        "total_demand_kwh": row.total_demand_kwh,
        "avg_demand_kwh": row.avg_demand_kwh,
        "avg_duration_minutes": row.avg_duration_minutes,
        "min_duration_minutes": row.min_duration_minutes,
        "max_duration_minutes": row.max_duration_minutes,
    }


def fetch_hourly_demand(session: Session) -> List[Dict[str, Any]]:
    """
    Retrieves all 24 hourly demand records, sorted by start_hour (0..23).
    Dashboard usage: Powers the 24h demand spline curve, session frequency chart, and Tab 2.
    CQL: SELECT ... FROM hourly_demand;
    """
    query = """
    SELECT start_hour, session_count, total_demand_kwh, avg_demand_kwh, avg_duration_minutes
    FROM hourly_demand;
    """
    rows = session.execute(query)
    results = [
        {
            "start_hour": r.start_hour,
            "session_count": r.session_count,
            "total_demand_kwh": r.total_demand_kwh,
            "avg_demand_kwh": r.avg_demand_kwh,
            "avg_duration_minutes": r.avg_duration_minutes,
        }
        for r in rows
    ]
    results.sort(key=lambda x: x["start_hour"])
    return results


def fetch_charger_type_metrics(session: Session) -> List[Dict[str, Any]]:
    """
    Retrieves comparative metrics for AC and DC charging architectures.
    Dashboard usage: Powers Tab 3 Charger Comparison table, volume/energy charts, and dwell ratios.
    CQL: SELECT ... FROM charger_type_metrics;
    """
    query = """
    SELECT charger_current_type, session_count, total_demand_kwh, avg_demand_kwh, avg_duration_minutes
    FROM charger_type_metrics;
    """
    rows = session.execute(query)
    results = [
        {
            "charger_current_type": r.charger_current_type,
            "session_count": r.session_count,
            "total_demand_kwh": r.total_demand_kwh,
            "avg_demand_kwh": r.avg_demand_kwh,
            "avg_duration_minutes": r.avg_duration_minutes,
        }
        for r in rows
    ]
    results.sort(key=lambda x: x["charger_current_type"])
    return results


def fetch_location_metrics(session: Session) -> List[Dict[str, Any]]:
    """
    Retrieves facility category metrics, sorted descending by transaction session_count.
    Dashboard usage: Powers Tab 4 Siting Leaderboard and horizontal ranking bar chart.
    CQL: SELECT ... FROM location_metrics;
    """
    query = """
    SELECT location, session_count, total_demand_kwh, avg_demand_kwh, avg_duration_minutes
    FROM location_metrics;
    """
    rows = session.execute(query)
    results = [
        {
            "location": r.location,
            "session_count": r.session_count,
            "total_demand_kwh": r.total_demand_kwh,
            "avg_demand_kwh": r.avg_demand_kwh,
            "avg_duration_minutes": r.avg_duration_minutes,
        }
        for r in rows
    ]
    results.sort(key=lambda x: x["session_count"], reverse=True)
    return results
