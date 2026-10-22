"""
Load script for inserting PySpark-generated analytics CSVs into Apache Cassandra.
Reads aggregate CSV outputs from data/processed/analytics/ using Python's standard csv module
and performs idempotent parameterized CQL insertions.
"""

import csv
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.database.cassandra_client import (
    create_tables,
    get_cassandra_session,
    insert_charger_type_metrics,
    insert_hourly_demand,
    insert_location_metrics,
    insert_overall_metrics,
    shutdown_cassandra,
)


def load_overall_metrics_from_csv(csv_path: Path) -> dict:
    """Reads overall_metrics.csv and returns a mapped dictionary with converted types."""
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        row = next(reader)
        return {
            "metric_id": "overall",
            "total_sessions": int(row["total_sessions"]),
            "total_demand_kwh": float(row["total_demand_kwh"]),
            "avg_demand_kwh": float(row["avg_demand_kwh"]),
            "avg_duration_minutes": float(row["avg_duration_minutes"]),
            "min_duration_minutes": float(row["min_duration_minutes"]),
            "max_duration_minutes": float(row["max_duration_minutes"]),
        }


def load_hourly_demand_from_csv(csv_path: Path) -> list:
    """Reads hourly_demand.csv and returns a list of dictionaries with converted types."""
    rows = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "start_hour": int(row["start_hour"]),
                    "session_count": int(row["session_count"]),
                    "total_demand_kwh": float(row["total_demand_kwh"]),
                    "avg_demand_kwh": float(row["avg_demand_kwh"]),
                    "avg_duration_minutes": float(row["avg_duration_minutes"]),
                }
            )
    return rows


def load_charger_type_metrics_from_csv(csv_path: Path) -> list:
    """Reads charger_type_metrics.csv and returns a list of dictionaries with converted types."""
    rows = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "charger_current_type": str(row["charger_current_type"]).strip(),
                    "session_count": int(row["session_count"]),
                    "total_demand_kwh": float(row["total_demand_kwh"]),
                    "avg_demand_kwh": float(row["avg_demand_kwh"]),
                    "avg_duration_minutes": float(row["avg_duration_minutes"]),
                }
            )
    return rows


def load_location_metrics_from_csv(csv_path: Path) -> list:
    """Reads top_locations.csv and returns a list of dictionaries with converted types."""
    rows = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "location": str(row["Location"]).strip(),
                    "session_count": int(row["session_count"]),
                    "total_demand_kwh": float(row["total_demand_kwh"]),
                    "avg_demand_kwh": float(row["avg_demand_kwh"]),
                    "avg_duration_minutes": float(row["avg_duration_minutes"]),
                }
            )
    return rows


def main():
    analytics_dir = PROJECT_ROOT / "data" / "processed" / "analytics"

    overall_csv = analytics_dir / "overall_metrics.csv"
    hourly_csv = analytics_dir / "hourly_demand.csv"
    type_csv = analytics_dir / "charger_type_metrics.csv"
    location_csv = analytics_dir / "top_locations.csv"

    # Check input files
    for path in [overall_csv, hourly_csv, type_csv, location_csv]:
        if not path.exists():
            print(f"[FAIL] Missing analytics CSV: {path}")
            print("       Please run 'python scripts/run_etl.py' first to generate analytics.")
            sys.exit(1)

    print("=" * 80)
    print(" EV CHARGING STATION ANALYTICS - CASSANDRA DATA LOADER ")
    print("=" * 80)

    cluster, session = None, None
    try:
        print("[*] Connecting to Apache Cassandra cluster...")
        cluster, session = get_cassandra_session()
        print("[+] Connection established.")

        print("[*] Ensuring Cassandra schema and tables exist...")
        create_tables(session)
        print("[+] Keyspace 'ev_analytics' and all analytical tables verified.")

        # 1. Load Overall Metrics
        print("\n[*] Loading Table 1: overall_metrics...")
        overall_data = load_overall_metrics_from_csv(overall_csv)
        insert_overall_metrics(session, overall_data)
        print(f"    [+] Successfully loaded 1 overall portfolio record.")

        # 2. Load Hourly Demand Profile
        print("\n[*] Loading Table 2: hourly_demand...")
        hourly_data = load_hourly_demand_from_csv(hourly_csv)
        insert_hourly_demand(session, hourly_data)
        print(f"    [+] Successfully loaded {len(hourly_data)} hourly profile records.")

        # 3. Load Charger Type Metrics
        print("\n[*] Loading Table 3: charger_type_metrics...")
        type_data = load_charger_type_metrics_from_csv(type_csv)
        insert_charger_type_metrics(session, type_data)
        print(f"    [+] Successfully loaded {len(type_data)} charger type records (AC, DC).")

        # 4. Load Location Metrics
        print("\n[*] Loading Table 4: location_metrics...")
        location_data = load_location_metrics_from_csv(location_csv)
        insert_location_metrics(session, location_data)
        print(f"    [+] Successfully loaded {len(location_data)} location category records.")

        print("\n" + "=" * 80)
        print(" [SUCCESS] All analytical results persisted to Cassandra tables ")
        print("=" * 80)

    except Exception as exc:
        print(f"\n[FAIL] Cassandra loader error: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        shutdown_cassandra(cluster, session)
        print("[*] Cassandra connection closed cleanly.")


if __name__ == "__main__":
    main()
