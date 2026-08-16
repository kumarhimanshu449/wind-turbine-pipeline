"""Explicit input schemas for deterministic CSV ingestion."""

from pyspark.sql.types import StringType, StructField, StructType

RAW_MEASUREMENT_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("turbine_id", StringType(), True),
        StructField("wind_speed", StringType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("power_output", StringType(), True),
    ]
)
