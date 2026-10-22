"""
Unit tests for dashboard formatting and insight calculation helpers.
Independent of Streamlit and Cassandra runtimes.
"""

import unittest
from src.dashboard.dashboard_helpers import (
    calculate_insights,
    format_duration_minutes,
    format_energy_kwh,
    format_number,
)


class TestDashboardHelpers(unittest.TestCase):
    def test_format_number(self):
        self.assertEqual(format_number(1234567), "1,234,567")
        self.assertEqual(format_number(1234.5678, decimals=2), "1,234.57")
        self.assertEqual(format_number(0), "0")
        self.assertEqual(format_number(None), "N/A")

    def test_format_energy_kwh(self):
        self.assertEqual(format_energy_kwh(500), "500.0 kWh")
        self.assertIn("MWh", format_energy_kwh(25000))
        self.assertIn("GWh", format_energy_kwh(1267066.08))
        self.assertEqual(format_energy_kwh(None), "N/A")

    def test_format_duration_minutes(self):
        self.assertEqual(format_duration_minutes(36.5), "36.5 min")
        self.assertIn("2h 33m", format_duration_minutes(152.7))
        self.assertEqual(format_duration_minutes(None), "N/A")

    def test_calculate_insights_empty(self):
        insights = calculate_insights(None, [], [], [])
        self.assertFalse(insights["has_data"])
        self.assertIsNone(insights["peak_hour"])

    def test_calculate_insights_populated(self):
        overall = {
            "total_sessions": 72108,
            "total_demand_kwh": 1267066.08,
            "avg_demand_kwh": 17.57,
            "avg_duration_minutes": 152.73,
        }
        hourly = [
            {"start_hour": 3, "session_count": 133, "total_demand_kwh": 2707.33, "avg_demand_kwh": 20.36, "avg_duration_minutes": 209.86},
            {"start_hour": 8, "session_count": 4109, "total_demand_kwh": 72436.47, "avg_demand_kwh": 17.63, "avg_duration_minutes": 147.36},
            {"start_hour": 18, "session_count": 6435, "total_demand_kwh": 118979.74, "avg_demand_kwh": 18.49, "avg_duration_minutes": 172.09},
        ]
        charger_types = [
            {"charger_current_type": "AC", "session_count": 57398, "total_demand_kwh": 984205.3, "avg_demand_kwh": 17.15, "avg_duration_minutes": 182.49},
            {"charger_current_type": "DC", "session_count": 14710, "total_demand_kwh": 282860.78, "avg_demand_kwh": 19.23, "avg_duration_minutes": 36.59},
        ]
        locations = [
            {"location": "public area", "session_count": 13952, "total_demand_kwh": 251898.51, "avg_demand_kwh": 18.05, "avg_duration_minutes": 134.34},
            {"location": "apartment", "session_count": 13913, "total_demand_kwh": 261764.0, "avg_demand_kwh": 18.81, "avg_duration_minutes": 206.7},
        ]

        insights = calculate_insights(overall, hourly, charger_types, locations)

        self.assertTrue(insights["has_data"])
        self.assertEqual(insights["peak_hour"], 18)
        self.assertAlmostEqual(insights["peak_demand_kwh"], 118979.74)
        self.assertEqual(insights["valley_hour"], 3)
        self.assertEqual(insights["top_location"], "public area")
        self.assertEqual(insights["top_location_sessions"], 13952)
        self.assertEqual(insights["duration_ratio_ac_to_dc"], 5.0)  # 182.49 / 36.59 ~= 5.0


if __name__ == "__main__":
    unittest.main()
