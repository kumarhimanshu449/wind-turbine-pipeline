from datetime import date, datetime

from wind_turbine_pipeline.storage import SparkTableStore


def test_store_persists_queryable_tables(spark):
    database = "wind_energy_test"
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    cleaned = spark.createDataFrame(
        [(datetime(2022, 3, 1), date(2022, 3, 1), 1)],
        ["timestamp", "measurement_date", "turbine_id"],
    )
    scores = spark.createDataFrame(
        [
            (date(2022, 3, 1), 1, 3.0, False),
            (date(2022, 3, 1), 2, 4.5, True),
        ],
        ["measurement_date", "turbine_id", "avg_power_output_mw", "is_anomaly"],
    )
    rejected = spark.createDataFrame(
        [("bad-date", "invalid timestamp")],
        ["raw_timestamp", "rejection_reason"],
    )

    try:
        SparkTableStore(spark, database).persist_outputs(cleaned, scores, rejected)

        assert spark.table(f"{database}.cleaned_measurements").count() == 1
        assert spark.table(f"{database}.turbine_daily_summary").count() == 2
        assert spark.table(f"{database}.turbine_daily_anomalies").count() == 1
        assert spark.table(f"{database}.rejected_records").count() == 1
    finally:
        spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
