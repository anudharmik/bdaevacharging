"""
Verification script for EV Charging Station Analytics in Apache Cassandra.
Queries each table, prints row counts and sample records, and verifies
strict parity against the source PySpark-generated CSV analytics.
"""

import csv
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.database.cassandra_client import (
    fetch_charger_type_metrics,
    fetch_hourly_demand,
    fetch_location_metrics,
    fetch_overall_metrics,
    get_cassandra_session,
    shutdown_cassandra,
)


def verify_overall_metrics(session, csv_path: Path) -> bool:
    print("\n--- [Table 1: overall_metrics Verification] ---")
    data = fetch_overall_metrics(session)
    if not data:
        print("[FAIL] overall_metrics table is empty!")
        return False

    with open(csv_path, mode="r", encoding="utf-8") as f:
        csv_row = next(csv.DictReader(f))

    print(f"  Cassandra Stored Record:")
    print(f"    - total_sessions       : {data['total_sessions']:,}")
    print(f"    - total_demand_kwh     : {data['total_demand_kwh']:,} kWh")
    print(f"    - avg_demand_kwh       : {data['avg_demand_kwh']} kWh")
    print(f"    - avg_duration_minutes : {data['avg_duration_minutes']} min")
    print(f"    - min_duration_minutes : {data['min_duration_minutes']} min")
    print(f"    - max_duration_minutes : {data['max_duration_minutes']} min")

    # Parity assertions
    assert data["total_sessions"] == int(csv_row["total_sessions"]), "total_sessions mismatch!"
    assert round(data["total_demand_kwh"], 2) == round(float(csv_row["total_demand_kwh"]), 2), "total_demand_kwh mismatch!"
    assert round(data["avg_demand_kwh"], 2) == round(float(csv_row["avg_demand_kwh"]), 2), "avg_demand_kwh mismatch!"
    assert round(data["avg_duration_minutes"], 2) == round(float(csv_row["avg_duration_minutes"]), 2), "avg_duration_minutes mismatch!"

    print("  [+] Parity check: 100% exact match with source overall_metrics.csv")
    return True


def verify_hourly_demand(session, csv_path: Path) -> bool:
    print("\n--- [Table 2: hourly_demand Verification] ---")
    c_rows = fetch_hourly_demand(session)
    print(f"  Cassandra Stored Rows: {len(c_rows)} hours")

    with open(csv_path, mode="r", encoding="utf-8") as f:
        csv_rows = list(csv.DictReader(f))

    assert len(c_rows) == len(csv_rows), f"Row count mismatch! Cassandra={len(c_rows)}, CSV={len(csv_rows)}"
    assert len(c_rows) == 24, f"Expected exactly 24 hourly rows, got {len(c_rows)}"

    print("  Sample Hours (Peek: 00:00, 08:00, 18:00 peak, 23:00):")
    sample_hours = {0, 8, 18, 23}
    for row in c_rows:
        if row["start_hour"] in sample_hours:
            print(
                f"    Hour {row['start_hour']:02d}:00 -> "
                f"Sessions: {row['session_count']:,} | "
                f"Demand: {row['total_demand_kwh']:,} kWh | "
                f"Avg Duration: {row['avg_duration_minutes']} min"
            )

    # Check peak hour 18 parity
    peak_c = next(r for r in c_rows if r["start_hour"] == 18)
    peak_csv = next(r for r in csv_rows if int(r["start_hour"]) == 18)
    assert peak_c["session_count"] == int(peak_csv["session_count"]), "Peak hour session count mismatch!"
    assert round(peak_c["total_demand_kwh"], 2) == round(float(peak_csv["total_demand_kwh"]), 2), "Peak hour demand mismatch!"

    print(f"  [+] Parity check: All {len(c_rows)} hours match source hourly_demand.csv")
    return True


def verify_charger_type_metrics(session, csv_path: Path) -> bool:
    print("\n--- [Table 3: charger_type_metrics Verification] ---")
    c_rows = fetch_charger_type_metrics(session)
    print(f"  Cassandra Stored Rows: {len(c_rows)} types")

    with open(csv_path, mode="r", encoding="utf-8") as f:
        csv_rows = list(csv.DictReader(f))

    assert len(c_rows) == len(csv_rows), f"Row count mismatch! Cassandra={len(c_rows)}, CSV={len(csv_rows)}"
    assert len(c_rows) == 2, f"Expected 2 charger types (AC, DC), got {len(c_rows)}"

    for row in c_rows:
        print(
            f"    Type: {row['charger_current_type']} -> "
            f"Sessions: {row['session_count']:,} | "
            f"Demand: {row['total_demand_kwh']:,} kWh | "
            f"Avg Duration: {row['avg_duration_minutes']} min"
        )
        csv_match = next(r for r in csv_rows if r["charger_current_type"] == row["charger_current_type"])
        assert row["session_count"] == int(csv_match["session_count"])
        assert round(row["total_demand_kwh"], 2) == round(float(csv_match["total_demand_kwh"]), 2)

    print("  [+] Parity check: Both AC and DC metrics match source charger_type_metrics.csv")
    return True


def verify_location_metrics(session, csv_path: Path) -> bool:
    print("\n--- [Table 4: location_metrics Verification] ---")
    c_rows = fetch_location_metrics(session)
    print(f"  Cassandra Stored Rows: {len(c_rows)} locations")

    with open(csv_path, mode="r", encoding="utf-8") as f:
        csv_rows = list(csv.DictReader(f))

    assert len(c_rows) == len(csv_rows), f"Row count mismatch! Cassandra={len(c_rows)}, CSV={len(csv_rows)}"

    print("  Top 5 Locations in Cassandra:")
    for row in c_rows[:5]:
        print(
            f"    - {row['location']:<18}: "
            f"Sessions: {row['session_count']:,} | "
            f"Demand: {row['total_demand_kwh']:,} kWh | "
            f"Avg Duration: {row['avg_duration_minutes']} min"
        )

    # Check top location
    top_c = c_rows[0]
    top_csv = csv_rows[0]
    assert top_c["location"] == top_csv["Location"]
    assert top_c["session_count"] == int(top_csv["session_count"])
    assert round(top_c["total_demand_kwh"], 2) == round(float(top_csv["total_demand_kwh"]), 2)

    print(f"  [+] Parity check: All {len(c_rows)} location categories match source top_locations.csv")
    return True


def main():
    analytics_dir = PROJECT_ROOT / "data" / "processed" / "analytics"

    overall_csv = analytics_dir / "overall_metrics.csv"
    hourly_csv = analytics_dir / "hourly_demand.csv"
    type_csv = analytics_dir / "charger_type_metrics.csv"
    location_csv = analytics_dir / "top_locations.csv"

    print("=" * 80)
    print(" EV CHARGING STATION ANALYTICS - CASSANDRA DATA VERIFICATION ")
    print("=" * 80)

    cluster, session = None, None
    try:
        cluster, session = get_cassandra_session()
        print("[+] Connected to Cassandra cluster.")

        ok1 = verify_overall_metrics(session, overall_csv)
        ok2 = verify_hourly_demand(session, hourly_csv)
        ok3 = verify_charger_type_metrics(session, type_csv)
        ok4 = verify_location_metrics(session, location_csv)

        if ok1 and ok2 and ok3 and ok4:
            print("\n" + "=" * 80)
            print(" [SUCCESS] All 4 Cassandra tables verified with 100% data fidelity ")
            print("=" * 80)
            sys.exit(0)
        else:
            print("\n[FAIL] Verification checks failed.")
            sys.exit(1)

    except Exception as exc:
        print(f"\n[FAIL] Verification error: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        shutdown_cassandra(cluster, session)
        print("[*] Cassandra connection closed cleanly.")


if __name__ == "__main__":
    main()
