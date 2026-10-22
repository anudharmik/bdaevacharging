"""
Unit tests for CSV-to-Cassandra data mapping and type conversion.
Runs completely offline without requiring a live Cassandra instance.
"""

import tempfile
import unittest
from pathlib import Path

from scripts.load_cassandra import (
    load_charger_type_metrics_from_csv,
    load_hourly_demand_from_csv,
    load_location_metrics_from_csv,
    load_overall_metrics_from_csv,
)
from src.database.cassandra_client import EXPECTED_TABLE_COLUMNS


class TestCassandraDataMapping(unittest.TestCase):
    def test_overall_metrics_mapping(self):
        csv_content = (
            "total_sessions,total_demand_kwh,avg_demand_kwh,avg_duration_minutes,min_duration_minutes,max_duration_minutes\n"
            "72108,1267066.08,17.57,152.73,1.0,1573.0\n"
        )
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp.write(csv_content)
            tmp_path = Path(tmp.name)

        try:
            mapped = load_overall_metrics_from_csv(tmp_path)
            self.assertEqual(mapped["metric_id"], "overall")
            self.assertIsInstance(mapped["total_sessions"], int)
            self.assertEqual(mapped["total_sessions"], 72108)
            self.assertIsInstance(mapped["total_demand_kwh"], float)
            self.assertAlmostEqual(mapped["total_demand_kwh"], 1267066.08)
            self.assertIsInstance(mapped["avg_demand_kwh"], float)
            self.assertAlmostEqual(mapped["avg_demand_kwh"], 17.57)
            self.assertIsInstance(mapped["avg_duration_minutes"], float)
            self.assertAlmostEqual(mapped["avg_duration_minutes"], 152.73)
            self.assertIsInstance(mapped["min_duration_minutes"], float)
            self.assertAlmostEqual(mapped["min_duration_minutes"], 1.0)
            self.assertIsInstance(mapped["max_duration_minutes"], float)
            self.assertAlmostEqual(mapped["max_duration_minutes"], 1573.0)
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_hourly_demand_mapping(self):
        csv_content = (
            "start_hour,session_count,total_demand_kwh,avg_demand_kwh,avg_duration_minutes\n"
            "0,909,24252.11,26.68,259.57\n"
            "18,6435,118979.74,18.49,172.09\n"
        )
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp.write(csv_content)
            tmp_path = Path(tmp.name)

        try:
            rows = load_hourly_demand_from_csv(tmp_path)
            self.assertEqual(len(rows), 2)
            # Hour 0
            self.assertEqual(rows[0]["start_hour"], 0)
            self.assertIsInstance(rows[0]["start_hour"], int)
            self.assertEqual(rows[0]["session_count"], 909)
            self.assertIsInstance(rows[0]["session_count"], int)
            self.assertAlmostEqual(rows[0]["total_demand_kwh"], 24252.11)
            # Hour 18
            self.assertEqual(rows[1]["start_hour"], 18)
            self.assertEqual(rows[1]["session_count"], 6435)
            self.assertAlmostEqual(rows[1]["total_demand_kwh"], 118979.74)
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_charger_type_metrics_mapping(self):
        csv_content = (
            "charger_current_type,session_count,total_demand_kwh,avg_demand_kwh,avg_duration_minutes\n"
            "AC,57398,984205.3,17.15,182.49\n"
            "DC,14710,282860.78,19.23,36.59\n"
        )
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp.write(csv_content)
            tmp_path = Path(tmp.name)

        try:
            rows = load_charger_type_metrics_from_csv(tmp_path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["charger_current_type"], "AC")
            self.assertEqual(rows[0]["session_count"], 57398)
            self.assertAlmostEqual(rows[0]["total_demand_kwh"], 984205.3)
            self.assertEqual(rows[1]["charger_current_type"], "DC")
            self.assertEqual(rows[1]["session_count"], 14710)
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_location_metrics_mapping(self):
        csv_content = (
            "Location,session_count,total_demand_kwh,avg_demand_kwh,avg_duration_minutes\n"
            "public area,13952,251898.51,18.05,134.34\n"
            "apartment,13913,261764.0,18.81,206.7\n"
        )
        with tempfile.NamedTemporaryFile("w", delete=False, suffix=".csv") as tmp:
            tmp.write(csv_content)
            tmp_path = Path(tmp.name)

        try:
            rows = load_location_metrics_from_csv(tmp_path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["location"], "public area")
            self.assertEqual(rows[0]["session_count"], 13952)
            self.assertAlmostEqual(rows[0]["total_demand_kwh"], 251898.51)
            self.assertEqual(rows[1]["location"], "apartment")
        finally:
            tmp_path.unlink(missing_ok=True)

    def test_expected_table_columns_definition(self):
        self.assertIn("overall_metrics", EXPECTED_TABLE_COLUMNS)
        self.assertIn("hourly_demand", EXPECTED_TABLE_COLUMNS)
        self.assertIn("charger_type_metrics", EXPECTED_TABLE_COLUMNS)
        self.assertIn("location_metrics", EXPECTED_TABLE_COLUMNS)

        self.assertIn("metric_id", EXPECTED_TABLE_COLUMNS["overall_metrics"])
        self.assertIn("start_hour", EXPECTED_TABLE_COLUMNS["hourly_demand"])
        self.assertIn("charger_current_type", EXPECTED_TABLE_COLUMNS["charger_type_metrics"])
        self.assertIn("location", EXPECTED_TABLE_COLUMNS["location_metrics"])


if __name__ == "__main__":
    unittest.main()
