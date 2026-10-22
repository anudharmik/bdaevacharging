"""
Unit tests for the PySpark EV Charging ETL pipeline.
Uses an isolated in-memory Spark DataFrame with known synthetic inputs.
Completely independent of external CSV downloads or network access.
"""

import unittest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.analytics.spark_session import get_spark_session, stop_spark_session
from src.analytics.etl_pipeline import (
    clean_charging_data,
    compute_overall_metrics,
    compute_hourly_demand,
    compute_charger_type_metrics,
    compute_top_locations,
    compute_duration_by_type,
    count_missing_values,
)


class TestETLPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initializes a shared local SparkSession for the test suite."""
        cls.spark = get_spark_session(
            app_name="TestETLPipeline",
            master="local[1]",
            driver_memory="512m",
        )

    @classmethod
    def tearDownClass(cls):
        """Stops the SparkSession cleanly."""
        stop_spark_session(cls.spark)

    def _create_sample_raw_df(self):
        """Creates a tiny 5-row synthetic DataFrame covering valid and edge cases."""
        schema = StructType(
            [
                StructField("UserID", IntegerType(), True),
                StructField("ChargerID", IntegerType(), True),
                StructField("ChargerCompany", IntegerType(), True),
                StructField("Location", StringType(), True),
                StructField("ChargerType", IntegerType(), True),
                StructField("StartDay", StringType(), True),
                StructField("StartTime", StringType(), True),
                StructField("EndDay", StringType(), True),
                StructField("EndTime", StringType(), True),
                StructField("StartDatetime", StringType(), True),
                StructField("EndDatetime", StringType(), True),
                StructField("Duration", DoubleType(), True),
                StructField("Demand", DoubleType(), True),
            ]
        )

        rows = [
            # 1. Valid AC session (Type 0, 120 mins, 15 kWh, hour 8, office)
            (10, 101, 1, "office", 0, "2022-05-10", "08:00:00", "2022-05-10", "10:00:00", "2022-05-10 08:00", "2022-05-10 10:00", 120.0, 15.0),
            # 2. Valid DC session (Type 1, 30 mins, 25 kWh, hour 8, highway)
            (20, 102, 1, "highway", 1, "2022-05-10", "08:30:00", "2022-05-10", "09:00:00", "2022-05-10 08:30", "2022-05-10 09:00", 30.0, 25.0),
            # 3. Exact duplicate of row 1 (should be dropped in deduplication)
            (10, 101, 1, "office", 0, "2022-05-10", "08:00:00", "2022-05-10", "10:00:00", "2022-05-10 08:00", "2022-05-10 10:00", 120.0, 15.0),
            # 4. Invalid negative duration anomaly (should be filtered)
            (30, 103, 1, "mall", 0, "2022-05-10", "11:00:00", "2022-05-10", "10:50:00", "2022-05-10 11:00", "2022-05-10 10:50", -10.0, 10.0),
            # 5. Invalid zero demand (should be filtered)
            (40, 104, 1, "home", 0, "2022-05-10", "14:00:00", "2022-05-10", "15:00:00", "2022-05-10 14:00", "2022-05-10 15:00", 60.0, 0.0),
        ]

        return self.spark.createDataFrame(rows, schema)

    def test_null_check(self):
        df = self._create_sample_raw_df()
        null_counts = count_missing_values(df)
        self.assertEqual(null_counts["UserID"], 0)
        self.assertEqual(null_counts["Duration"], 0)
        self.assertEqual(null_counts["Demand"], 0)

    def test_clean_charging_data(self):
        df = self._create_sample_raw_df()
        cleaned_df, metrics = clean_charging_data(df)

        self.assertEqual(metrics["raw_count"], 5)
        self.assertEqual(metrics["duplicate_rows_removed"], 1)
        self.assertEqual(metrics["invalid_rows_removed"], 2)
        self.assertEqual(metrics["cleaned_count"], 2)

        # Check added columns
        columns = cleaned_df.columns
        self.assertIn("start_timestamp", columns)
        self.assertIn("end_timestamp", columns)
        self.assertIn("start_hour", columns)
        self.assertIn("charger_current_type", columns)

        # Check values
        records = cleaned_df.orderBy("UserID").collect()
        self.assertEqual(records[0]["charger_current_type"], "AC")
        self.assertEqual(records[0]["start_hour"], 8)
        self.assertEqual(records[1]["charger_current_type"], "DC")
        self.assertEqual(records[1]["start_hour"], 8)

    def test_compute_overall_metrics(self):
        df = self._create_sample_raw_df()
        cleaned_df, _ = clean_charging_data(df)
        overall = compute_overall_metrics(cleaned_df).first()

        self.assertEqual(overall["total_sessions"], 2)
        self.assertEqual(overall["total_demand_kwh"], 40.0)  # 15.0 + 25.0
        self.assertEqual(overall["avg_demand_kwh"], 20.0)    # 40 / 2
        self.assertEqual(overall["avg_duration_minutes"], 75.0)  # (120 + 30) / 2
        self.assertEqual(overall["min_duration_minutes"], 30.0)
        self.assertEqual(overall["max_duration_minutes"], 120.0)

    def test_compute_hourly_demand(self):
        df = self._create_sample_raw_df()
        cleaned_df, _ = clean_charging_data(df)
        hourly = compute_hourly_demand(cleaned_df).collect()

        self.assertEqual(len(hourly), 1)
        self.assertEqual(hourly[0]["start_hour"], 8)
        self.assertEqual(hourly[0]["session_count"], 2)
        self.assertEqual(hourly[0]["total_demand_kwh"], 40.0)

    def test_compute_charger_type_metrics(self):
        df = self._create_sample_raw_df()
        cleaned_df, _ = clean_charging_data(df)
        type_metrics = compute_charger_type_metrics(cleaned_df).orderBy("charger_current_type").collect()

        self.assertEqual(len(type_metrics), 2)
        # AC
        self.assertEqual(type_metrics[0]["charger_current_type"], "AC")
        self.assertEqual(type_metrics[0]["session_count"], 1)
        self.assertEqual(type_metrics[0]["total_demand_kwh"], 15.0)
        self.assertEqual(type_metrics[0]["avg_duration_minutes"], 120.0)
        # DC
        self.assertEqual(type_metrics[1]["charger_current_type"], "DC")
        self.assertEqual(type_metrics[1]["session_count"], 1)
        self.assertEqual(type_metrics[1]["total_demand_kwh"], 25.0)
        self.assertEqual(type_metrics[1]["avg_duration_minutes"], 30.0)

    def test_compute_top_locations(self):
        df = self._create_sample_raw_df()
        cleaned_df, _ = clean_charging_data(df)
        top_locs = compute_top_locations(cleaned_df, limit_n=5).collect()

        self.assertEqual(len(top_locs), 2)
        locations = {r["Location"] for r in top_locs}
        self.assertEqual(locations, {"office", "highway"})

    def test_compute_duration_by_type(self):
        df = self._create_sample_raw_df()
        cleaned_df, _ = clean_charging_data(df)
        duration_stats = compute_duration_by_type(cleaned_df).orderBy("charger_current_type").collect()

        self.assertEqual(len(duration_stats), 2)
        self.assertEqual(duration_stats[0]["charger_current_type"], "AC")
        self.assertEqual(duration_stats[0]["avg_duration_minutes"], 120.0)
        self.assertEqual(duration_stats[1]["charger_current_type"], "DC")
        self.assertEqual(duration_stats[1]["avg_duration_minutes"], 30.0)


if __name__ == "__main__":
    unittest.main()
