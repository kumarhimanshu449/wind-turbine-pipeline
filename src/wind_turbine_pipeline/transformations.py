"""Pure DataFrame transformations used by the turbine pipeline."""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from wind_turbine_pipeline.config import PipelineConfig

MEASUREMENT_COLUMNS = ("wind_speed", "wind_direction", "power_output")

def _blank_to_null(column_name: str) -> F.Column:
    value = F.trim(F.col(column_name))
    return F.when(value == "", F.lit(None)).otherwise(value)


def standardize_raw(raw: DataFrame) -> DataFrame:
    """Parse raw strings while retaining source values for auditability."""

    return raw.select(
        F.expr(
            "try_to_timestamp(nullif(trim(timestamp), ''), "
            "'yyyy-MM-dd HH:mm:ss')"
        ).alias("timestamp"),
        F.expr("try_cast(nullif(trim(turbine_id), '') as int)").alias("turbine_id"),
        F.expr("try_cast(nullif(trim(wind_speed), '') as double)").alias(
            "wind_speed"
        ),
        F.expr("try_cast(nullif(trim(wind_direction), '') as double)").alias(
            "wind_direction"
        ),
        F.expr("try_cast(nullif(trim(power_output), '') as double)").alias(
            "power_output"
        ),
        _blank_to_null("timestamp").alias("raw_timestamp"),
        _blank_to_null("turbine_id").alias("raw_turbine_id"),
        _blank_to_null("wind_speed").alias("raw_wind_speed"),
        _blank_to_null("wind_direction").alias("raw_wind_direction"),
        _blank_to_null("power_output").alias("raw_power_output"),
        F.col("source_file"),
    )


def split_rejected_records(standardized: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Separate records whose business key cannot be recovered."""

    rejection_reason = F.concat_ws(
        "; ",
        F.when(F.col("timestamp").isNull(), F.lit("invalid timestamp")),
        F.when(
            F.col("turbine_id").isNull() | (F.col("turbine_id") <= 0),
            F.lit("invalid turbine_id"),
        ),
    )
    with_reason = standardized.withColumn("rejection_reason", rejection_reason)
    rejected = with_reason.filter(F.length("rejection_reason") > 0)
    valid = with_reason.filter(F.length("rejection_reason") == 0).drop(
        "rejection_reason"
    )
    return valid, rejected


def deduplicate_measurements(valid: DataFrame) -> DataFrame:
    """Keep one deterministic, most-complete row per turbine and timestamp."""

    completeness = reduce(
        lambda left, right: left + right,
        [
            F.when(F.col(name).isNotNull(), 1).otherwise(0)
            for name in MEASUREMENT_COLUMNS
        ],
    )
    fingerprint = F.sha2(
        F.concat_ws(
            "|",
            *[
                F.coalesce(F.col(name).cast("string"), F.lit("<null>"))
                for name in (*MEASUREMENT_COLUMNS, "source_file")
            ],
        ),
        256,
    )
    rank = Window.partitionBy("timestamp", "turbine_id").orderBy(
        F.col("_completeness").desc(),
        F.col("source_file").asc_nulls_last(),
        F.col("_fingerprint").asc(),
    )
    return (
        valid.withColumn("_completeness", completeness)
        .withColumn("_fingerprint", fingerprint)
        .withColumn("_row_number", F.row_number().over(rank))
        .filter(F.col("_row_number") == 1)
        .drop("_completeness", "_fingerprint", "_row_number")
    )


def sanitize_measurements(data: DataFrame, config: PipelineConfig) -> DataFrame:
    """Null impossible readings so they can be imputed without hiding anomalies."""

    wind_speed_invalid = F.col("wind_speed").isNotNull() & (
        (F.col("wind_speed") < 0)
        | (F.col("wind_speed") > F.lit(config.max_wind_speed_mps))
    )
    wind_direction_invalid = F.col("wind_direction").isNotNull() & (
        (F.col("wind_direction") < 0) | (F.col("wind_direction") >= 360)
    )
    power_output_invalid = F.col("power_output").isNotNull() & (
        (F.col("power_output") < 0)
        | (F.col("power_output") > F.lit(config.rated_power_mw))
    )

    return (
        data.withColumn(
            "wind_speed_was_outlier",
            wind_speed_invalid
            | (F.col("raw_wind_speed").isNotNull() & F.col("wind_speed").isNull()),
        )
        .withColumn(
            "wind_direction_was_outlier",
            wind_direction_invalid
            | (
                F.col("raw_wind_direction").isNotNull()
                & F.col("wind_direction").isNull()
            ),
        )
        .withColumn(
            "power_output_was_outlier",
            power_output_invalid
            | (
                F.col("raw_power_output").isNotNull()
                & F.col("power_output").isNull()
            ),
        )
        .withColumn(
            "wind_speed",
            F.when(wind_speed_invalid, F.lit(None)).otherwise(F.col("wind_speed")),
        )
        .withColumn(
            "wind_direction",
            F.when(wind_direction_invalid, F.lit(None)).otherwise(
                F.col("wind_direction")
            ),
        )
        .withColumn(
            "power_output",
            F.when(power_output_invalid, F.lit(None)).otherwise(F.col("power_output")),
        )
    )


def filter_date_range(data: DataFrame, config: PipelineConfig) -> DataFrame:
    """Apply an optional inclusive measurement-date range."""

    filtered = data
    if config.start_date:
        filtered = filtered.filter(F.to_date("timestamp") >= F.lit(config.start_date))
    if config.end_date:
        filtered = filtered.filter(F.to_date("timestamp") <= F.lit(config.end_date))
    return filtered


def complete_hourly_grid(
    data: DataFrame,
    config: PipelineConfig,
    turbine_roster: DataFrame | None = None,
) -> DataFrame:
    """Create the expected 24 hourly slots for every turbine and date.

    The assignment describes one complete 24-hour batch per day. This function
    therefore fills missing sensor rows for every observed turbine across the
    selected inclusive date range.
    """

    observed_bounds = data.agg(
        F.min(F.to_date("timestamp")).alias("observed_start_date"),
        F.max(F.to_date("timestamp")).alias("observed_end_date"),
    )
    bounds = observed_bounds.select(
        (
            F.lit(config.start_date).cast("date")
            if config.start_date
            else F.col("observed_start_date")
        ).alias("start_date"),
        (
            F.lit(config.end_date).cast("date")
            if config.end_date
            else F.col("observed_end_date")
        ).alias("end_date"),
    )
    dates = bounds.select(
        F.explode(F.sequence("start_date", "end_date")).alias("measurement_date")
    )
    turbines = (
        turbine_roster.select("turbine_id").distinct()
        if turbine_roster is not None
        else data.select("turbine_id").distinct()
    )
    hours = data.sparkSession.range(24).select(
        F.col("id").cast("int").alias("hour_of_day")
    )
    expected = (
        turbines.crossJoin(dates)
        .crossJoin(hours)
        .withColumn(
            "timestamp",
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_format("measurement_date", "yyyy-MM-dd"),
                    F.format_string("%02d:00:00", F.col("hour_of_day")),
                ),
                "yyyy-MM-dd HH:mm:ss",
            ),
        )
        .drop("hour_of_day")
    )
    observed = data.withColumn("_observed_record", F.lit(True)).drop(
        "measurement_date"
    )
    return (
        expected.join(observed, ["timestamp", "turbine_id"], "left")
        .withColumn("is_gap_filled", F.col("_observed_record").isNull())
        .drop("_observed_record")
    )


def _circular_direction(sin_value: F.Column, cos_value: F.Column) -> F.Column:
    return F.when(
        sin_value.isNull() | cos_value.isNull(), F.lit(None).cast("double")
    ).otherwise(
        F.pmod(F.degrees(F.atan2(sin_value, cos_value)) + F.lit(360.0), F.lit(360.0))
    )


def impute_measurements(data: DataFrame) -> DataFrame:
    """Hierarchically impute nulls with no driver-side collection.

    Wind speed and power output use turbine-day, then turbine, then fleet
    medians. Direction uses equivalent circular means so values around 0/360
    degrees are handled correctly.
    """

    keyed = data.withColumn("measurement_date", F.to_date("timestamp"))
    median_columns = ("wind_speed", "power_output")

    daily = keyed.groupBy("turbine_id", "measurement_date").agg(
        *[
            F.percentile_approx(name, 0.5, 10_000).alias(f"_daily_{name}")
            for name in median_columns
        ],
        F.avg(F.sin(F.radians("wind_direction"))).alias("_daily_direction_sin"),
        F.avg(F.cos(F.radians("wind_direction"))).alias("_daily_direction_cos"),
    )
    turbine = keyed.groupBy("turbine_id").agg(
        *[
            F.percentile_approx(name, 0.5, 10_000).alias(f"_turbine_{name}")
            for name in median_columns
        ],
        F.avg(F.sin(F.radians("wind_direction"))).alias("_turbine_direction_sin"),
        F.avg(F.cos(F.radians("wind_direction"))).alias("_turbine_direction_cos"),
    )
    fleet = keyed.agg(
        *[
            F.percentile_approx(name, 0.5, 10_000).alias(f"_fleet_{name}")
            for name in median_columns
        ],
        F.avg(F.sin(F.radians("wind_direction"))).alias("_fleet_direction_sin"),
        F.avg(F.cos(F.radians("wind_direction"))).alias("_fleet_direction_cos"),
    )

    joined = keyed.join(daily, ["turbine_id", "measurement_date"]).join(
        turbine, ["turbine_id"]
    ).crossJoin(fleet)

    for name in median_columns:
        imputed_value = F.coalesce(
            F.col(name),
            F.col(f"_daily_{name}"),
            F.col(f"_turbine_{name}"),
            F.col(f"_fleet_{name}"),
        )
        joined = joined.withColumn(
            f"{name}_was_imputed", F.col(name).isNull() & imputed_value.isNotNull()
        ).withColumn(name, imputed_value)

    direction_value = F.coalesce(
        F.col("wind_direction"),
        _circular_direction(
            F.col("_daily_direction_sin"), F.col("_daily_direction_cos")
        ),
        _circular_direction(
            F.col("_turbine_direction_sin"), F.col("_turbine_direction_cos")
        ),
        _circular_direction(
            F.col("_fleet_direction_sin"), F.col("_fleet_direction_cos")
        ),
    )
    joined = joined.withColumn(
        "wind_direction_was_imputed",
        F.col("wind_direction").isNull() & direction_value.isNotNull(),
    ).withColumn("wind_direction", direction_value)

    boolean_columns = (
        "wind_speed_was_outlier",
        "wind_direction_was_outlier",
        "power_output_was_outlier",
    )
    for name in boolean_columns:
        joined = joined.withColumn(name, F.coalesce(F.col(name), F.lit(False)))

    return joined.select(
        "timestamp",
        "measurement_date",
        "turbine_id",
        "wind_speed",
        "wind_direction",
        "power_output",
        "source_file",
        "is_gap_filled",
        "wind_speed_was_imputed",
        "wind_direction_was_imputed",
        "power_output_was_imputed",
        "wind_speed_was_outlier",
        "wind_direction_was_outlier",
        "power_output_was_outlier",
    )


def calculate_daily_summary(cleaned: DataFrame) -> DataFrame:
    """Calculate one 24-hour power summary per turbine."""

    summary = cleaned.groupBy("turbine_id", "measurement_date").agg(
        F.count(F.lit(1)).alias("records_expected"),
        F.sum(F.when(~F.col("is_gap_filled"), 1).otherwise(0)).alias(
            "records_observed"
        ),
        F.count("power_output").alias("power_measurements_available"),
        F.sum(F.col("power_output_was_imputed").cast("int")).alias(
            "power_measurements_imputed"
        ),
        F.sum(F.col("power_output_was_outlier").cast("int")).alias(
            "power_outliers_cleaned"
        ),
        F.min("power_output").alias("min_power_output_mw"),
        F.max("power_output").alias("max_power_output_mw"),
        F.avg("power_output").alias("avg_power_output_mw"),
        F.stddev_samp("power_output").alias("power_output_stddev_mw"),
    )
    return (
        summary.withColumn(
            "data_completeness_ratio",
            F.col("records_observed") / F.col("records_expected"),
        )
        .withColumn("window_start", F.col("measurement_date").cast("timestamp"))
        .withColumn("window_end", F.expr("window_start + INTERVAL 24 HOURS"))
        .select(
            "window_start",
            "window_end",
            "measurement_date",
            "turbine_id",
            "records_expected",
            "records_observed",
            "data_completeness_ratio",
            "power_measurements_available",
            "power_measurements_imputed",
            "power_outliers_cleaned",
            "min_power_output_mw",
            "max_power_output_mw",
            "avg_power_output_mw",
            "power_output_stddev_mw",
        )
    )


def add_anomaly_scores(summary: DataFrame, threshold: float = 2.0) -> DataFrame:
    """Score each turbine-day average against the same day's turbine fleet."""

    fleet = summary.groupBy("measurement_date").agg(
        F.avg("avg_power_output_mw").alias("fleet_avg_power_output_mw"),
        F.stddev_samp("avg_power_output_mw").alias("fleet_stddev_power_output_mw"),
    )
    scored = summary.join(fleet, ["measurement_date"], "left")
    usable_stddev = F.col("fleet_stddev_power_output_mw").isNotNull() & (
        F.col("fleet_stddev_power_output_mw") > 0
    )
    z_score = F.when(
        usable_stddev,
        (F.col("avg_power_output_mw") - F.col("fleet_avg_power_output_mw"))
        / F.col("fleet_stddev_power_output_mw"),
    )
    return (
        scored.withColumn("anomaly_z_score", z_score)
        .withColumn(
            "lower_anomaly_bound_mw",
            F.col("fleet_avg_power_output_mw")
            - F.lit(threshold) * F.col("fleet_stddev_power_output_mw"),
        )
        .withColumn(
            "upper_anomaly_bound_mw",
            F.col("fleet_avg_power_output_mw")
            + F.lit(threshold) * F.col("fleet_stddev_power_output_mw"),
        )
        .withColumn(
            "is_anomaly",
            F.when(
                usable_stddev, F.abs(F.col("anomaly_z_score")) > threshold
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "anomaly_direction",
            F.when(F.col("is_anomaly") & (F.col("anomaly_z_score") > 0), "HIGH")
            .when(F.col("is_anomaly"), "LOW")
            .otherwise("NORMAL"),
        )
    )
