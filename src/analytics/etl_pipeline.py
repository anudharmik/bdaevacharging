"""
PySpark ETL Pipeline for Electric Vehicle (EV) Charging Station Analytics.
Provides explicit, reproducible data cleaning and core analytical aggregations.
Structured into clear sequential stages designed for straightforward viva explanation.

Stages:
1. Define explicit schema matching raw transaction records.
2. Load raw CSV with schema enforcement (avoiding expensive inferSchema scans).
3. Check and verify missing (null) values.
4. Clean dataset: deduplication, timestamp parsing, start-hour extraction,
   AC/DC mapping, and invalid duration/demand filtering.
5. Compute analytical aggregations:
   - Overall portfolio summary
   - 24-hour demand profile
   - Charger architecture breakdown (AC vs DC)
   - Facility siting leaderboard (top locations)
   - Duration bounds by charger type
"""

from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    hour,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# Stage 1: Explicit Schema Definition
# =============================================================================


def get_raw_schema() -> StructType:
    """
    Explicit StructType schema matching the 13 columns in ChargingRecords.csv.
    Specifying types prevents expensive two-pass schema inference scans and
    guarantees correct data types for distributed PySpark execution.
    """
    return StructType(
        [
            StructField("UserID", IntegerType(), True),
            StructField("ChargerID", IntegerType(), True),
            StructField("ChargerCompany", IntegerType(), True),
            StructField("Location", StringType(), True),
            StructField("ChargerType", IntegerType(), True),  # 0 = AC (Slow), 1 = DC (Fast)
            StructField("StartDay", StringType(), True),
            StructField("StartTime", StringType(), True),
            StructField("EndDay", StringType(), True),
            StructField("EndTime", StringType(), True),
            StructField("StartDatetime", StringType(), True),
            StructField("EndDatetime", StringType(), True),
            StructField("Duration", DoubleType(), True),  # Total session dwell in minutes
            StructField("Demand", DoubleType(), True),    # Total energy delivered in kilowatt-hours (kWh)
        ]
    )


# =============================================================================
# Stage 2: Data Ingestion
# =============================================================================


def load_raw_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """
    Loads raw CSV transaction file using the explicit schema with header enabled.
    """
    return (
        spark.read.option("header", "true")
        .schema(get_raw_schema())
        .csv(csv_path)
    )


# =============================================================================
# Stage 3: Data Quality & Null Auditing
# =============================================================================


def count_missing_values(df: DataFrame) -> Dict[str, int]:
    """
    Computes null counts across all columns in the DataFrame in a single distributed pass.
    """
    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    row = df.select(null_exprs).first()
    return row.asDict() if row else {}


# =============================================================================
# Stage 4: Cleaning & Feature Engineering
# =============================================================================


def clean_charging_data(df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Cleans raw charging data conservatively and deterministically.

    Transformations:
    1. Deduplication: drop exact duplicate rows (caused by telemetry re-transmission).
    2. Timestamp Parsing: combine StartDay and StartTime for second-precision timestamps.
    3. Start Hour Extraction: extract hour (0-23) from start timestamp for 24h load profiling.
    4. Categorical Mapping: map numeric ChargerType (0 -> 'AC', 1 -> 'DC').
    5. Validation Filter: remove invalid records (Duration <= 0, Demand <= 0, or null timestamps).

    Returns:
        (cleaned_df, metrics_dict)
    """
    raw_count = df.count()

    # Step 1: Remove exact duplicate rows
    deduped_df = df.dropDuplicates()
    deduped_count = deduped_df.count()
    duplicate_rows_removed = raw_count - deduped_count

    # Step 2: Timestamp parsing and feature derivation
    with_features_df = (
        deduped_df.withColumn(
            "start_timestamp",
            to_timestamp(concat_ws(" ", col("StartDay"), col("StartTime")), "yyyy-MM-dd H:m:s"),
        )
        .withColumn(
            "end_timestamp",
            to_timestamp(concat_ws(" ", col("EndDay"), col("EndTime")), "yyyy-MM-dd H:m:s"),
        )
        .withColumn("start_hour", hour(col("start_timestamp")))
        .withColumn(
            "charger_current_type",
            when(col("ChargerType") == 0, "AC")
            .when(col("ChargerType") == 1, "DC")
            .otherwise("UNKNOWN"),
        )
    )

    # Step 3: Filter out anomalous sessions (Duration <= 0 or Demand <= 0)
    cleaned_df = with_features_df.filter(
        (col("Duration") > 0.0)
        & (col("Demand") > 0.0)
        & col("start_timestamp").isNotNull()
        & col("end_timestamp").isNotNull()
    )

    clean_count = cleaned_df.count()
    invalid_rows_removed = deduped_count - clean_count

    metrics = {
        "raw_count": raw_count,
        "duplicate_rows_removed": duplicate_rows_removed,
        "invalid_rows_removed": invalid_rows_removed,
        "total_removed": raw_count - clean_count,
        "cleaned_count": clean_count,
    }

    return cleaned_df, metrics


# =============================================================================
# Stage 5: Analytical Aggregations (Core Business Metrics)
# =============================================================================


def compute_overall_metrics(cleaned_df: DataFrame) -> DataFrame:
    """
    1. Overall Portfolio Summary:
       Computes total sessions, total energy demand, average demand, and duration bounds.
       Stored in Cassandra table: overall_metrics
    """
    return cleaned_df.select(
        count("*").alias("total_sessions"),
        spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
        spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
        spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        spark_round(spark_min("Duration"), 2).alias("min_duration_minutes"),
        spark_round(spark_max("Duration"), 2).alias("max_duration_minutes"),
    )


def compute_hourly_demand(cleaned_df: DataFrame) -> DataFrame:
    """
    2. Hourly Energy Demand Profile (24-Hour Cycle):
       Groups sessions by start_hour (00:00 to 23:00) to model driver arrival grid demand.
       Stored in Cassandra table: hourly_demand
    """
    return (
        cleaned_df.groupBy("start_hour")
        .agg(
            count("*").alias("session_count"),
            spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
            spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("start_hour")
    )


def compute_charger_type_metrics(cleaned_df: DataFrame) -> DataFrame:
    """
    3. Charger Architecture Utilization (AC Level 2 vs DC Fast):
       Compares session volume, total energy throughput, and average dwell time.
       Stored in Cassandra table: charger_type_metrics
    """
    return (
        cleaned_df.groupBy("charger_current_type")
        .agg(
            count("*").alias("session_count"),
            spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
            spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("charger_current_type")
    )


def compute_top_locations(cleaned_df: DataFrame, limit_n: int = 10) -> DataFrame:
    """
    4. Facility Category Siting Leaderboard:
       Ranks installation environments (apartment, public area, hotel, etc.) by session frequency.
       Stored in Cassandra table: location_metrics
    """
    return (
        cleaned_df.groupBy("Location")
        .agg(
            count("*").alias("session_count"),
            spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
            spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        )
        .orderBy(col("session_count").desc())
        .limit(limit_n)
    )


def compute_duration_by_type(cleaned_df: DataFrame) -> DataFrame:
    """
    5. Session Duration Bounds by Charger Architecture:
       Computes min, mean, and max duration across AC and DC charging modes.
    """
    return (
        cleaned_df.groupBy("charger_current_type")
        .agg(
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
            spark_round(spark_min("Duration"), 2).alias("min_duration_minutes"),
            spark_round(spark_max("Duration"), 2).alias("max_duration_minutes"),
        )
        .orderBy("charger_current_type")
    )
