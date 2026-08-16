"""Command-line entry point for local and scheduled execution."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from wind_turbine_pipeline.config import PipelineConfig
from wind_turbine_pipeline.pipeline import run_pipeline
from wind_turbine_pipeline.storage import SparkTableStore

LOGGER = logging.getLogger(__name__)

def create_spark_session(warehouse_dir: str) -> SparkSession:
    """Create a UTC Spark session with a persistent local Hive metastore."""

    warehouse = Path(warehouse_dir).resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    metastore = warehouse.parent / "spark-metastore"
    return (
        SparkSession.builder.appName("wind-turbine-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", warehouse.as_uri())
        .config("spark.ui.enabled", "false")
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )
        .enableHiveSupport()
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="CSV paths or globs, for example data/raw/*.csv",
    )
    parser.add_argument(
        "--warehouse-dir",
        default="build/spark-warehouse",
        help="Directory for the local Parquet-backed Spark warehouse",
    )
    parser.add_argument("--database", default="wind_energy")
    parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD filter")
    parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD filter")
    parser.add_argument("--rated-power-mw", type=float, default=5.0)
    parser.add_argument("--max-wind-speed-mps", type=float, default=60.0)
    parser.add_argument("--anomaly-threshold", type=float, default=2.0)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    config = PipelineConfig(
        max_wind_speed_mps=args.max_wind_speed_mps,
        rated_power_mw=args.rated_power_mw,
        anomaly_stddev_threshold=args.anomaly_threshold,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    spark = create_spark_session(args.warehouse_dir)
    try:
        result = run_pipeline(spark, args.input, config)
        result.cleaned.cache()
        result.daily_summary.cache()
        store = SparkTableStore(spark, args.database)
        store.persist_outputs(
            result.cleaned,
            result.anomaly_scores,
            result.rejected,
        )
        LOGGER.info("cleaned_measurements rows: %s", result.cleaned.count())
        LOGGER.info("turbine_daily_summary rows: %s", result.daily_summary.count())
        LOGGER.info(
            "turbine_daily_anomalies rows: %s",
            result.anomaly_scores.filter("is_anomaly").count(),
        )
        LOGGER.info("rejected_records rows: %s", result.rejected.count())
        LOGGER.info("Stored tables in Spark database '%s'", args.database)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
