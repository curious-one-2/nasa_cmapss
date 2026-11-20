# import sys
# from pathlib import Path

# # Automatically add src/ to Python's search path
# root_dir = Path(__file__).resolve().parent
# src_dir = root_dir / "src"
# if str(src_dir) not in sys.path:
#     sys.path.insert(0, str(src_dir))
    
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from nasa_platform.transforms.silver.cmapss_silver import clean_and_validate_cmapss

@pytest.fixture(scope="session")
def spark():
    """Provides a local Spark session for unit testing transforms."""
    return SparkSession.builder \
        .appName("CmapssSilverUnitTest") \
        .master("local[2]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def test_clean_and_validate_cmapss_valid_records(spark):
    schema = StructType([
        StructField("unit_id", IntegerType(), True),
        StructField("time_cycle", IntegerType(), True),
        StructField("setting_1", FloatType(), True),
        StructField("sensor_1", FloatType(), True)
    ])
    
    data = [(1, 1, 0.99, 518.67)]
    df = spark.createDataFrame(data, schema)
    
    result_df = clean_and_validate_cmapss(df)
    results = result_df.collect()
    
    assert len(results) == 1
    assert results[0]["is_valid"] is True
    assert results[0]["validation_error"] is None

def test_clean_and_validate_cmapss_invalid_records(spark):
    schema = StructType([
        StructField("unit_id", IntegerType(), True),
        StructField("time_cycle", IntegerType(), True),
        StructField("setting_1", FloatType(), True),
        StructField("sensor_1", FloatType(), True)
    ])
    
    # Test cases: null unit_id, zero time_cycle
    data = [
        (None, 1, 0.99, 518.67),
        (1, 0, 0.99, 518.67)
    ]
    df = spark.createDataFrame(data, schema)
    
    result_df = clean_and_validate_cmapss(df)
    results = result_df.collect()
    
    assert len(results) == 2
    assert results[0]["is_valid"] is False
    assert "unit_id" in results[0]["validation_error"]
    assert results[1]["is_valid"] is False
    assert "time_cycle" in results[1]["validation_error"]

def test_clean_and_validate_cmapss_deduplication(spark):
    schema = StructType([
        StructField("unit_id", IntegerType(), True),
        StructField("time_cycle", IntegerType(), True),
        StructField("setting_1", FloatType(), True),
        StructField("sensor_1", FloatType(), True)
    ])
    
    # Duplicate (unit_id=1, time_cycle=1)
    data = [
        (1, 1, 0.99, 518.67),
        (1, 1, 0.99, 518.67)
    ]
    df = spark.createDataFrame(data, schema)
    
    result_df = clean_and_validate_cmapss(df)
    assert result_df.count() == 1