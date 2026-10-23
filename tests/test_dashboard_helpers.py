"""
Unit tests for dashboard formatting, filtering, and insight calculation helpers.
Independent of Streamlit and Cassandra runtimes.
"""

import unittest
import pandas as pd

from src.dashboard.dashboard_helpers import (
    aggregate_chargers_from_df,
    aggregate_hourly_from_df,
    aggregate_locations_from_df,
    calculate_filtered_insights,
    calculate_insights,
    compute_kpis_from_df,
    filter_records,
    format_duration_minutes,
    format_energy_kwh,
    format_number,
    get_metric_peak_valley,
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

    def test_get_metric_peak_valley(self):
        hourly = [
            {"start_hour": 2, "session_count": 50, "total_demand_kwh": 500.0},
            {"start_hour": 12, "session_count": 800, "total_demand_kwh": 9500.0},
            {"start_hour": 18, "session_count": 1200, "total_demand_kwh": 15000.0},
        ]
        p_hr, p_val, v_hr, v_val = get_metric_peak_valley(hourly, "total_demand_kwh")
        self.assertEqual(p_hr, 18)
        self.assertEqual(p_val, 15000.0)
        self.assertEqual(v_hr, 2)
        self.assertEqual(v_val, 500.0)

        # Empty data handling
        p_hr_e, p_val_e, v_hr_e, v_val_e = get_metric_peak_valley([], "total_demand_kwh")
        self.assertIsNone(p_hr_e)
        self.assertEqual(p_val_e, 0.0)

    # -------------------------------------------------------------------------
    # Multi-dimensional Filtering & Aggregation Tests
    # -------------------------------------------------------------------------

    def _sample_df(self):
        return pd.DataFrame([
            {"charger_current_type": "AC", "Location": "apartment", "start_hour": 8, "Demand": 20.0, "Duration": 180.0},
            {"charger_current_type": "AC", "Location": "apartment", "start_hour": 18, "Demand": 25.0, "Duration": 240.0},
            {"charger_current_type": "DC", "Location": "public area", "start_hour": 18, "Demand": 30.0, "Duration": 40.0},
            {"charger_current_type": "DC", "Location": "hotel", "start_hour": 22, "Demand": 15.0, "Duration": 30.0},
        ])

    def test_filter_records_all(self):
        df = self._sample_df()
        res = filter_records(df, charger_type="All", location="All", start_hour_range=(0, 23))
        self.assertEqual(len(res), 4)

    def test_filter_records_charger_type(self):
        df = self._sample_df()
        res_ac = filter_records(df, charger_type="AC", location="All", start_hour_range=(0, 23))
        self.assertEqual(len(res_ac), 2)
        self.assertTrue((res_ac["charger_current_type"] == "AC").all())

        res_dc = filter_records(df, charger_type="DC (Fast)", location="All", start_hour_range=(0, 23))
        self.assertEqual(len(res_dc), 2)
        self.assertTrue((res_dc["charger_current_type"] == "DC").all())

    def test_filter_records_location_and_hour(self):
        df = self._sample_df()
        res = filter_records(df, charger_type="All", location="apartment", start_hour_range=(10, 20))
        self.assertEqual(len(res), 1)
        self.assertEqual(res.iloc[0]["start_hour"], 18)

    def test_filter_records_empty_result(self):
        df = self._sample_df()
        res = filter_records(df, charger_type="DC", location="apartment", start_hour_range=(0, 23))
        self.assertEqual(len(res), 0)

    def test_compute_kpis_from_df(self):
        df = self._sample_df()
        kpis = compute_kpis_from_df(df, portfolio_total_sessions=10)
        self.assertTrue(kpis["has_data"])
        self.assertEqual(kpis["total_sessions"], 4)
        self.assertEqual(kpis["total_demand_kwh"], 90.0)
        self.assertEqual(kpis["avg_demand_kwh"], 22.5)  # 90.0 / 4
        self.assertEqual(kpis["avg_duration_minutes"], 122.5)  # (180+240+40+30)/4 = 490/4 = 122.5
        self.assertEqual(kpis["min_duration_minutes"], 30.0)
        self.assertEqual(kpis["max_duration_minutes"], 240.0)
        self.assertEqual(kpis["ac_sessions"], 2)
        self.assertEqual(kpis["dc_sessions"], 2)
        self.assertEqual(kpis["ac_share_pct"], 50.0)
        self.assertEqual(kpis["dc_share_pct"], 50.0)
        self.assertEqual(kpis["portfolio_share_pct"], 40.0)

    def test_compute_kpis_empty_df(self):
        empty_df = pd.DataFrame()
        kpis = compute_kpis_from_df(empty_df)
        self.assertFalse(kpis["has_data"])
        self.assertEqual(kpis["total_sessions"], 0)
        self.assertEqual(kpis["total_demand_kwh"], 0.0)
        self.assertEqual(kpis["avg_demand_kwh"], 0.0)
        self.assertEqual(kpis["avg_duration_minutes"], 0.0)
        self.assertEqual(kpis["min_duration_minutes"], 0.0)
        self.assertEqual(kpis["max_duration_minutes"], 0.0)

    def test_aggregate_hourly_from_df(self):
        df = self._sample_df()
        hourly = aggregate_hourly_from_df(df)
        self.assertEqual(len(hourly), 24)
        self.assertEqual(hourly[0]["start_hour"], 0)
        self.assertEqual(hourly[23]["start_hour"], 23)

        h8 = next(h for h in hourly if h["start_hour"] == 8)
        self.assertEqual(h8["session_count"], 1)
        self.assertEqual(h8["total_demand_kwh"], 20.0)

        h18 = next(h for h in hourly if h["start_hour"] == 18)
        self.assertEqual(h18["session_count"], 2)
        self.assertEqual(h18["total_demand_kwh"], 55.0)

        h0 = next(h for h in hourly if h["start_hour"] == 0)
        self.assertEqual(h0["session_count"], 0)
        self.assertEqual(h0["total_demand_kwh"], 0.0)

    def test_aggregate_chargers_from_df(self):
        df = self._sample_df()
        chargers = aggregate_chargers_from_df(df)
        self.assertEqual(len(chargers), 2)
        ac = next(c for c in chargers if c["charger_current_type"] == "AC")
        dc = next(c for c in chargers if c["charger_current_type"] == "DC")
        self.assertEqual(ac["session_count"], 2)
        self.assertEqual(ac["total_demand_kwh"], 45.0)
        self.assertEqual(dc["session_count"], 2)
        self.assertEqual(dc["total_demand_kwh"], 45.0)

    def test_aggregate_locations_from_df(self):
        df = self._sample_df()
        locs = aggregate_locations_from_df(df)
        self.assertEqual(len(locs), 3)
        self.assertEqual(locs[0]["location"], "apartment")
        self.assertEqual(locs[0]["session_count"], 2)
        self.assertEqual(locs[0]["share_pct"], 50.0)

    def test_calculate_filtered_insights(self):
        df = self._sample_df()
        kpis = compute_kpis_from_df(df)
        hourly = aggregate_hourly_from_df(df)
        chargers = aggregate_chargers_from_df(df)
        locs = aggregate_locations_from_df(df)

        insights = calculate_filtered_insights(kpis, hourly, chargers, locs)
        self.assertTrue(insights["has_data"])
        self.assertEqual(insights["peak_hour"], 18)
        self.assertEqual(insights["peak_demand_kwh"], 55.0)
        self.assertEqual(insights["top_location"], "apartment")
        self.assertEqual(insights["duration_ratio_ac_to_dc"], 6.0)  # AC avg = 210, DC avg = 35 -> 210/35 = 6.0


if __name__ == "__main__":
    unittest.main()
