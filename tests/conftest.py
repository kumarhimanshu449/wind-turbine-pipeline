import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("wind-turbine-pipeline-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
