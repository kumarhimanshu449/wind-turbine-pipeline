"""Pipeline orchestration, kept separate from the CLI for testability."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from wind_turbine_pipeline.config import PipelineConfig
from wind_turbine_pipeline.schemas import RAW_MEASUREMENT_SCHEMA
from wind_turbine_pipeline.transformations import (
    add_anomaly_scores,
    calculate_daily_summary,
    complete_hourly_grid,
    deduplicate_measurements,
    filter_date_range,
    impute_measurements,
    sanitize_measurements,
    split_rejected_records,
    standardize_raw,
)


@dataclass(frozen=True)
class PipelineResult:
    cleaned: DataFrame
    daily_summary: DataFrame
    anomaly_scores: DataFrame
    rejected: DataFrame


def read_csv_measurements(
    spark: SparkSession, input_paths: str | Sequence[str]
) -> DataFrame:
    """Read one or more CSV paths with a non-inferred schema."""

    reader = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_MEASUREMENT_SCHEMA)
    )
    raw = reader.csv(input_paths)
    return raw.withColumn("source_file", F.input_file_name())


def transform(raw: DataFrame, config: PipelineConfig | None = None) -> PipelineResult:
    """Run all deterministic transformations without storage side effects."""

    config = config or PipelineConfig()
    standardized = standardize_raw(raw)
    valid, rejected = split_rejected_records(standardized)
    turbine_roster = valid.select("turbine_id").distinct()
    selected = filter_date_range(valid, config)
    deduplicated = deduplicate_measurements(selected)
    sanitized = sanitize_measurements(deduplicated, config)
    completed = complete_hourly_grid(sanitized, config, turbine_roster)
    cleaned = impute_measurements(completed)
    summary = calculate_daily_summary(cleaned)
    scored = add_anomaly_scores(summary, config.anomaly_stddev_threshold)
    return PipelineResult(cleaned, summary, scored, rejected)


def run_pipeline(
    spark: SparkSession,
    input_paths: str | Sequence[str],
    config: PipelineConfig | None = None,
) -> PipelineResult:
    """Read CSV measurements and return the transformed datasets."""

    return transform(read_csv_measurements(spark, input_paths), config)
