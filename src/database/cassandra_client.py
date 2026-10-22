"""
Apache Cassandra client module for EV Charging Station Analytics.
Provides straightforward, explicit CQL functions using the official DataStax python driver.
Avoids ORMs, metaclasses, and generic abstractions for straightforward viva explainability.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

# Load environment configuration if available
try:
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv(
            dotenv_path=Path(__file__).resolve().parent.parent.parent / ".env.example"
        )
except ImportError:
    pass

DEFAULT_HOST = os.environ.get("CASSANDRA_HOST", "localhost")
DEFAULT_PORT = int(os.environ.get("CASSANDRA_PORT", 9042))
DEFAULT_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "ev_analytics")
DEFAULT_USER = os.environ.get("CASSANDRA_USERNAME", "cassandra")
DEFAULT_PASSWORD = os.environ.get("CASSANDRA_PASSWORD", "cassandra")


def get_cassandra_session(
    host: Optional[str] = None,
    port: Optional[int] = None,
    keyspace: Optional[str] = DEFAULT_KEYSPACE,
) -> Tuple[Cluster, Session]:
    """
    Connects to the Apache Cassandra cluster and returns (cluster, session).
    Uses PlainTextAuthProvider and explicit timeouts.
    """
    target_host = host or DEFAULT_HOST
    target_port = port or DEFAULT_PORT

    auth_provider = PlainTextAuthProvider(
        username=DEFAULT_USER, password=DEFAULT_PASSWORD
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


def shutdown_cassandra(cluster: Optional[Cluster], session: Optional[Session]) -> None:
    """Closes session and cluster connection cleanly."""
    if session and not session.is_shutdown:
        session.shutdown()
    if cluster and not cluster.is_shutdown:
        cluster.shutdown()


# ==============================================================================
# Schema Management
# ==============================================================================


EXPECTED_TABLE_COLUMNS = {
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
    Checks whether any existing table has incompatible columns.
    Raises RuntimeError if a discrepancy is detected instead of dropping tables.
    """
    query = "SELECT table_name, column_name FROM system_schema.columns WHERE keyspace_name = %s;"
    rows = session.execute(query, [keyspace])
    existing_tables: Dict[str, set] = {}
    for r in rows:
        existing_tables.setdefault(r.table_name, set()).add(r.column_name)

    for table_name, expected_cols in EXPECTED_TABLE_COLUMNS.items():
        if table_name in existing_tables:
            actual_cols = existing_tables[table_name]
            # Check that all expected columns exist in the existing table
            missing_cols = expected_cols - actual_cols
            if missing_cols:
                raise RuntimeError(
                    f"Incompatible schema detected in table '{keyspace}.{table_name}'. "
                    f"Missing expected columns: {missing_cols}. Existing columns: {actual_cols}. "
                    "Refusing to drop or overwrite without manual review."
                )


def create_tables(session: Session) -> None:
    """
    Creates keyspace and the four core analytical tables if they do not exist.
    Verifies existing schemas first to prevent data corruption.
    """
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS ev_analytics
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        AND durable_writes = true;
        """
    )
    session.set_keyspace("ev_analytics")

    # Verify compatibility if tables already exist
    verify_schema_compatibility(session, "ev_analytics")

    # 1. Overall portfolio summary table
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

    # 2. Hourly demand profile table
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

    # 3. Charger type (AC/DC) utilization table
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

    # 4. Location category metrics table
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


# ==============================================================================
# Insertion Functions (Explicit, Parameterized, Idempotent)
# ==============================================================================


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
    """Inserts or updates hourly demand profile rows."""
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
    """Inserts or updates charger type (AC/DC) metrics rows."""
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
    """Inserts or updates location category metrics rows."""
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


# ==============================================================================
# Retrieval Functions (Explicit CQL SELECT queries)
# ==============================================================================


def fetch_overall_metrics(session: Session) -> Optional[Dict[str, Any]]:
    """Retrieves the overall metrics summary."""
    row = session.execute(
        "SELECT metric_id, total_sessions, total_demand_kwh, avg_demand_kwh, "
        "avg_duration_minutes, min_duration_minutes, max_duration_minutes "
        "FROM overall_metrics WHERE metric_id = 'overall';"
    ).one()
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
    """Retrieves all 24 hourly demand rows, sorted by start_hour."""
    rows = session.execute(
        "SELECT start_hour, session_count, total_demand_kwh, avg_demand_kwh, avg_duration_minutes "
        "FROM hourly_demand;"
    )
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
    """Retrieves charger type metrics (AC and DC)."""
    rows = session.execute(
        "SELECT charger_current_type, session_count, total_demand_kwh, avg_demand_kwh, avg_duration_minutes "
        "FROM charger_type_metrics;"
    )
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
    """Retrieves location metrics sorted descending by session_count."""
    rows = session.execute(
        "SELECT location, session_count, total_demand_kwh, avg_demand_kwh, avg_duration_minutes "
        "FROM location_metrics;"
    )
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
