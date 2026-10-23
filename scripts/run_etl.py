"""
ETL Runner Script for EV Charging Station Analytics.
Executes the PySpark data ingestion, cleaning, analytical aggregation,
and exports processed artifacts to data/processed/.
"""

import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.analytics.spark_session import get_spark_session, stop_spark_session
from src.analytics.etl_pipeline import (
    load_raw_data,
    count_missing_values,
    clean_charging_data,
    compute_overall_metrics,
    compute_hourly_demand,
    compute_charger_type_metrics,
    compute_top_locations,
    compute_duration_by_type,
)


def export_dataframe_to_csv(df, output_csv_path: Path):
    """
    Saves a small analytical summary DataFrame as a single CSV file with a header.
    Using pandas for small summary tables (e.g. 2 to 24 rows) avoids creating
    fragmented Spark part-files on local storage while maintaining pure CSV output.
    """
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(output_csv_path, index=False)
    print(f"    Saved: {output_csv_path.relative_to(PROJECT_ROOT)} ({len(pdf)} rows)")


def main():
    raw_data_path = PROJECT_ROOT / "data" / "raw" / "ChargingRecords.csv"
    processed_dir = PROJECT_ROOT / "data" / "processed"
    analytics_dir = processed_dir / "analytics"

    if not raw_data_path.exists():
        print(f"[FAIL] Raw dataset not found at: {raw_data_path}")
        print("       Please download it first as instructed in README.md")
        sys.exit(1)

    print("=" * 80)
    print(" EV CHARGING STATION ANALYTICS - PYSPARK ETL PIPELINE ")
    print("=" * 80)

    spark = None
    try:
        print("\n[*] Initializing local SparkSession...")
        spark = get_spark_session(app_name="EVChargingAnalyticsETL", master="local[2]")
        print(f"[+] SparkSession initialized (Spark Version: {spark.version})")

        # 1. Ingestion
        print(f"\n[*] Step 1: Loading raw dataset from {raw_data_path.name}...")
        raw_df = load_raw_data(spark, str(raw_data_path))
        raw_count = raw_df.count()
        print(f"[+] Raw Records Loaded : {raw_count:,} rows")
        print("[+] Raw Schema Definition:")
        raw_df.printSchema()

        # 2. Null Check
        print("[*] Step 2: Checking for missing / NULL values...")
        null_counts = count_missing_values(raw_df)
        has_nulls = False
        for col_name, null_cnt in null_counts.items():
            if null_cnt > 0:
                print(f"    - Column '{col_name}': {null_cnt} missing values")
                has_nulls = True
        if not has_nulls:
            print("[+] Zero missing values found across all 13 columns.")

        # 3. Data Cleaning
        print("\n[*] Step 3: Cleaning dataset (deduplication, timestamp parsing, validation)...")
        cleaned_df, metrics = clean_charging_data(raw_df)

        print(f"    - Raw record count            : {metrics['raw_count']:,}")
        print(f"    - Exact duplicate rows dropped: {metrics['duplicate_rows_removed']:,}")
        print(f"    - Invalid duration/demand dropped: {metrics['invalid_rows_removed']:,}")
        print(f"    - Final cleaned record count  : {metrics['cleaned_count']:,}")
        retention_pct = (metrics['cleaned_count'] / metrics['raw_count']) * 100.0
        print(f"[+] Dataset retention rate        : {retention_pct:.2f}%")

        # 4. Save cleaned dataset
        cleaned_parquet_path = str(processed_dir / "cleaned_charging_records.parquet")
        print(f"\n[*] Step 4: Persisting cleaned dataset to Parquet: {processed_dir.name}/cleaned_charging_records.parquet...")
        cleaned_df.write.mode("overwrite").parquet(cleaned_parquet_path)
        print("[+] Cleaned dataset written successfully to Parquet format.")

        # 5. Core Analytics
        print("\n[*] Step 5: Computing core analytical aggregations...")

        # Analysis 1: Overall KPIs
        print("\n--- [Analysis 1: Overall Portfolio KPIs] ---")
        overall_df = compute_overall_metrics(cleaned_df)
        overall_df.show(truncate=False)
        export_dataframe_to_csv(overall_df, analytics_dir / "overall_metrics.csv")

        # Analysis 2: Hourly Demand
        print("\n--- [Analysis 2: Hourly Energy Demand Profile (24h)] ---")
        hourly_df = compute_hourly_demand(cleaned_df)
        hourly_df.show(24, truncate=False)
        export_dataframe_to_csv(hourly_df, analytics_dir / "hourly_demand.csv")

        # Analysis 3: Charger Type Breakdown (AC vs DC)
        print("\n--- [Analysis 3: Charger Type Metrics (AC vs DC)] ---")
        type_df = compute_charger_type_metrics(cleaned_df)
        type_df.show(truncate=False)
        export_dataframe_to_csv(type_df, analytics_dir / "charger_type_metrics.csv")

        # Analysis 4: Top 10 Locations
        print("\n--- [Analysis 4: Top 10 Locations by Session Volume] ---")
        top_loc_df = compute_top_locations(cleaned_df, limit_n=10)
        top_loc_df.show(truncate=False)
        export_dataframe_to_csv(top_loc_df, analytics_dir / "top_locations.csv")

        # Analysis 5: Duration Statistics by Charger Type
        print("\n--- [Analysis 5: Session Duration by Charger Type] ---")
        duration_df = compute_duration_by_type(cleaned_df)
        duration_df.show(truncate=False)
        export_dataframe_to_csv(duration_df, analytics_dir / "duration_by_type.csv")

        print("\n" + "=" * 80)
        print(" [SUCCESS] PySpark ETL Pipeline Completed Successfully ")
        print("=" * 80)

    except Exception as exc:
        print(f"\n[FAIL] Pipeline execution failed: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            print("\n[*] Stopping SparkSession...")
            stop_spark_session(spark)
            print("[+] SparkSession terminated cleanly.")


if __name__ == "__main__":
    main()
