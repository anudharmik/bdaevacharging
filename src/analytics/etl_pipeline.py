"""
PySpark ETL Pipeline for Electric Vehicle (EV) Charging Station Analytics.
Provides explicit, reproducible data cleaning and core analytical aggregations.
Designed for clarity and straightforward viva explanation.
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


def get_raw_schema() -> StructType:
    """
    Explicit schema matching the 13 columns present in the official ChargingRecords.csv.
    Specifying types prevents expensive schema inference scans and guarantees correct types.
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
            StructField("Duration", DoubleType(), True),  # Recorded session duration in minutes
            StructField("Demand", DoubleType(), True),    # Energy delivered in kilowatt-hours (kWh)
        ]
    )


def load_raw_data(spark: SparkSession, csv_path: str) -> DataFrame:
    """Loads raw CSV file using an explicit schema with header enabled."""
    return (
        spark.read.option("header", "true")
        .schema(get_raw_schema())
        .csv(csv_path)
    )


def count_missing_values(df: DataFrame) -> Dict[str, int]:
    """Computes null counts across all columns in the DataFrame."""
    null_exprs = [count(when(col(c).isNull(), c)).alias(c) for c in df.columns]
    row = df.select(null_exprs).first()
    return row.asDict() if row else {}


def clean_charging_data(df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    """
    Cleans raw charging data conservatively and deterministically.

    Steps:
    1. Drop exact duplicate rows.
    2. Parse high-precision timestamps from Day and Time fields.
    3. Extract charging start hour (0-23) for grid load profiling.
    4. Map ChargerType (0 -> 'AC', 1 -> 'DC') into a human-readable column.
    5. Filter out invalid sessions: duration <= 0, demand <= 0, or unparseable timestamps.

    Returns:
        (cleaned_df, metrics_dict)
    """
    raw_count = df.count()

    # Step 1: Drop exact duplicates
    deduped_df = df.dropDuplicates()
    deduped_count = deduped_df.count()
    duplicate_rows_removed = raw_count - deduped_count

    # Step 2: Timestamp parsing (combining StartDay and StartTime gives second-level precision)
    with_timestamps_df = (
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

    # Step 3: Conservative validation filter
    # Valid sessions must have positive duration (> 0 min) and delivered energy (> 0 kWh)
    cleaned_df = with_timestamps_df.filter(
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


# ==============================================================================
# Analytical Aggregations (Core Business Logic)
# ==============================================================================


def compute_overall_metrics(cleaned_df: DataFrame) -> DataFrame:
    """1. Computes overall project KPIs across all validated sessions."""
    return cleaned_df.select(
        count("*").alias("total_sessions"),
        spark_round(spark_sum("Demand"), 2).alias("total_demand_kwh"),
        spark_round(avg("Demand"), 2).alias("avg_demand_kwh"),
        spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
        spark_round(spark_min("Duration"), 2).alias("min_duration_minutes"),
        spark_round(spark_max("Duration"), 2).alias("max_duration_minutes"),
    )


def compute_hourly_demand(cleaned_df: DataFrame) -> DataFrame:
    """2. Computes session count and energy demand aggregated by hour of the day."""
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
    """3. Computes utilization, throughput, and session lengths across AC vs DC chargers."""
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
    """4. Identifies top charging locations by transaction frequency and energy delivery."""
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
    """5. Computes summary duration statistics broken down by AC and DC chargers."""
    return (
        cleaned_df.groupBy("charger_current_type")
        .agg(
            spark_round(avg("Duration"), 2).alias("avg_duration_minutes"),
            spark_round(spark_min("Duration"), 2).alias("min_duration_minutes"),
            spark_round(spark_max("Duration"), 2).alias("max_duration_minutes"),
        )
        .orderBy("charger_current_type")
    )
