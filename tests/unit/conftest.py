# tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
  """Initialize a reusable local SparkSession with Delta Lake extensions for

  unit testing.
  """
  spark_session = (
      SparkSession.builder.appName("nasa-pipeline-unit-tests")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog",
      )
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  )

  yield spark_session

  spark_session.stop()