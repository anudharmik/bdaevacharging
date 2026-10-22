"""
Apache Spark session manager for the EV Charging Station Analytics project.
Configures a lightweight, reproducible local PySpark session optimized for development
and viva demonstration on Windows workstations.
"""

import os
import sys
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "EVChargingStationAnalyticsETL",
    master: str = "local[2]",
    driver_memory: str = "1g",
) -> SparkSession:
    """
    Creates or retrieves an existing local SparkSession.

    Configurations applied:
    - Sets PYSPARK_PYTHON to current interpreter (Python 3.11) to avoid launching
      incompatible system Python versions (e.g., Python 3.14).
    - Sets spark.driver.host and bindAddress to 127.0.0.1 to avoid hostname resolution
      and Windows firewall timeouts between Spark driver and workers.
    - Sets HADOOP_HOME if available to eliminate Windows winutils warnings.
    """
    # Ensure worker processes use the current Python interpreter (.venv)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Ensure HADOOP_HOME points to base directory, not bin
    if os.environ.get("HADOOP_HOME", "").endswith(r"\bin"):
        os.environ["HADOOP_HOME"] = os.path.dirname(os.environ["HADOOP_HOME"])
    elif not os.environ.get("HADOOP_HOME") and os.path.isdir(r"C:\hadoop-3.2.4"):
        os.environ["HADOOP_HOME"] = r"C:\hadoop-3.2.4"

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")  # Disable web UI for batch tests
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """Safely terminates an active SparkSession."""
    if spark:
        try:
            spark.stop()
        except Exception:
            pass
