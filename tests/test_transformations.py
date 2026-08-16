from datetime import date, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from wind_turbine_pipeline.config import PipelineConfig
from wind_turbine_pipeline.transformations import (
    add_anomaly_scores,
    calculate_daily_summary,
    complete_hourly_grid,
    deduplicate_measurements,
    impute_measurements,
    sanitize_measurements,
    split_rejected_records,
    standardize_raw,
)

RAW_COLUMNS = [
    "timestamp",
    "turbine_id",
    "wind_speed",
    "wind_direction",
    "power_output",
    "source_file",
]

def test_standardisation_rejection_deduplication_and_outlier_cleaning(spark):
    raw = spark.createDataFrame(
        [
            ("2022-03-01 00:00:00", "1", "12", "180", None, "a.csv"),
            ("2022-03-01 00:00:00", "1", "12", "180", "3", "a.csv"),
            ("bad-date", "1", "12", "180", "3", "a.csv"),
            ("2022-03-01 01:00:00", "1", "999", "500", "99", "a.csv"),
        ],
        RAW_COLUMNS,
    )

    valid, rejected = split_rejected_records(standardize_raw(raw))
    deduplicated = deduplicate_measurements(valid)
    cleaned = sanitize_measurements(deduplicated, PipelineConfig())

    assert rejected.count() == 1
    assert deduplicated.count() == 2
    complete_duplicate = deduplicated.filter(F.hour("timestamp") == 0).first()
    assert complete_duplicate.power_output == 3.0

    invalid = cleaned.filter(F.hour("timestamp") == 1).first()
    assert invalid.wind_speed is None
    assert invalid.wind_direction is None
    assert invalid.power_output is None
    assert invalid.wind_speed_was_outlier is True
    assert invalid.wind_direction_was_outlier is True
    assert invalid.power_output_was_outlier is True


def test_missing_hour_is_created_and_imputed(spark):
    missing_hour = 5
    rows = []
    for hour in range(24):
        if hour != missing_hour:
            rows.append(
                (
                    datetime(2022, 3, 1) + timedelta(hours=hour),
                    1,
                    10.0,
                    350.0 if hour % 2 == 0 else 10.0,
                    3.0,
                    "a.csv",
                    False,
                    False,
                    False,
                )
            )
    columns = [
        "timestamp",
        "turbine_id",
        "wind_speed",
        "wind_direction",
        "power_output",
        "source_file",
        "wind_speed_was_outlier",
        "wind_direction_was_outlier",
        "power_output_was_outlier",
    ]
    sanitized = spark.createDataFrame(rows, columns)

    completed = complete_hourly_grid(sanitized, PipelineConfig())
    cleaned = impute_measurements(completed)

    assert cleaned.count() == 24
    filled = cleaned.filter(F.hour("timestamp") == missing_hour).first()
    assert filled.is_gap_filled is True
    assert filled.wind_speed == pytest.approx(10.0)
    assert filled.power_output == pytest.approx(3.0)

    # assert filled.wind_direction == pytest.approx(0.0, abs=1.0)

    circular_distance_from_zero = min(
        abs(filled.wind_direction),
        abs(360.0 - filled.wind_direction),
    )
    assert circular_distance_from_zero <= 1.0
    assert filled.power_output_was_imputed is True


def test_daily_summary_calculates_required_statistics(spark):
    cleaned = spark.createDataFrame(
        [
            (datetime(2022, 3, 1, 0), date(2022, 3, 1), 1, 2.0, False, False, False),
            (datetime(2022, 3, 1, 1), date(2022, 3, 1), 1, 4.0, False, False, False),
        ],
        [
            "timestamp",
            "measurement_date",
            "turbine_id",
            "power_output",
            "is_gap_filled",
            "power_output_was_imputed",
            "power_output_was_outlier",
        ],
    )

    result = calculate_daily_summary(cleaned).first()

    assert result.min_power_output_mw == pytest.approx(2.0)
    assert result.max_power_output_mw == pytest.approx(4.0)
    assert result.avg_power_output_mw == pytest.approx(3.0)
    assert result.records_observed == 2
    assert result.data_completeness_ratio == pytest.approx(1.0)


def test_anomaly_is_turbine_daily_average_beyond_two_fleet_stddevs(spark):
    rows = [(date(2022, 3, 1), turbine_id, 3.0) for turbine_id in range(1, 15)]
    rows.append((date(2022, 3, 1), 15, 9.0))
    summary = spark.createDataFrame(
        rows, ["measurement_date", "turbine_id", "avg_power_output_mw"]
    )

    scored = add_anomaly_scores(summary, threshold=2.0)
    anomalies = scored.filter("is_anomaly").collect()

    assert len(anomalies) == 1
    assert anomalies[0].turbine_id == 15
    assert anomalies[0].anomaly_direction == "HIGH"
    assert anomalies[0].anomaly_z_score > 2.0
