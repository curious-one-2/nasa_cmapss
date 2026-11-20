import pytest
from pyspark.sql import DataFrame, SparkSession
#from pyspark.sql.function import Functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from nasa_platform.ml.dataset_builder import build_ml_datasets

@pytest.fixture(scope="session")
def spark():
    """Provides a local Spark session for unit testing transforms."""
    return SparkSession.builder \
        .appName("TestingDatasetBuilder") \
        .master("local[2]") \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def test_dataset_builder(spark):


    schema = StructType([
        StructField("engine_id", IntegerType(), True),
        StructField("cycle", IntegerType(), True),
        StructField("rul", IntegerType(), True),
        StructField("sensor_2_roll_mean_5", DoubleType(), True),
    ])
    mock_data = [
        (1, 1, 150, 540.5),  # RUL > 125 -> should be capped at 125
        (1, 2, 140, 541.0),  # RUL > 125 -> should be capped at 125
        (2, 1, 100, 535.2),  # RUL <= 125 -> should remain 100
        (2, 2, 99, 535.5),   # RUL <= 125 -> should remain 99
    ]

    test_data_df = spark.createDataFrame(mock_data, schema=schema)

    # 2. Act: Run dataset builder with a strict cap of 125
    train_df, val_df = build_ml_datasets(test_data_df, train_ratio=0.5, max_rul_cap=125)

    train_rows = train_df.collect()
    val_rows = val_df.collect()

    # 3. Assert: Verify RUL capping occurred on filtered results
    all_rows = train_rows + val_rows
    for row in all_rows:
        assert row["target_rul"] <= 125

    # Check specific capped value for engine 1
    engine_1_targets = [r["target_rul"] for r in all_rows if r["engine_id"] == 1]
    assert all(t == 125 for t in engine_1_targets)

    # Check uncapped value for engine 2
    engine_2_targets = [r["target_rul"] for r in all_rows if r["engine_id"] == 2 and r["cycle"] == 1]
    assert engine_2_targets[0] == 100

    # 4. Assert: Verify engine-level segregation (no engine_id exists in both train and val simultaneously with this small hash)
    train_engines = {r["engine_id"] for r in train_rows}
    val_engines = {r["engine_id"] for r in val_rows}
    
    # Intersection of engines in train and val should be disjoint for strict data integrity
    assert train_engines.isdisjoint(val_engines)