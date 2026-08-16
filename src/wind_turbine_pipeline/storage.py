"""Storage adapter for Spark-managed analytical tables."""

import re

from pyspark.sql import DataFrame, SparkSession

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SparkTableStore:
    """Persist outputs as Parquet-backed tables in a Spark SQL database."""

    def __init__(self, spark: SparkSession, database: str = "wind_energy") -> None:
        if not _SAFE_IDENTIFIER.fullmatch(database):
            raise ValueError("database must be a valid unquoted SQL identifier")
        self.spark = spark
        self.database = database

    def _table_name(self, table: str) -> str:
        if not _SAFE_IDENTIFIER.fullmatch(table):
            raise ValueError("table must be a valid unquoted SQL identifier")
        return f"{self.database}.{table}"

    def initialise(self) -> None:
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def write(self, frame: DataFrame, table: str, partitioned: bool = True) -> None:
        writer = frame.write.format("parquet").mode("overwrite")
        if partitioned and "measurement_date" in frame.columns:
            writer = writer.partitionBy("measurement_date")
        writer.saveAsTable(self._table_name(table))

    def persist_outputs(
        self,
        cleaned: DataFrame,
        anomaly_scores: DataFrame,
        rejected: DataFrame,
    ) -> None:
        self.initialise()
        self.write(cleaned, "cleaned_measurements")
        self.write(anomaly_scores, "turbine_daily_summary")
        self.write(
            anomaly_scores.filter("is_anomaly"),
            "turbine_daily_anomalies",
        )
        self.write(rejected, "rejected_records", partitioned=False)
