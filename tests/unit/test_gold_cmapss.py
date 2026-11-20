import pytest
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as f
from pyspark.sql.types import  StructType, StructField, IntegerType, FloatType, StringType

from nasa_platform.transforms.gold.cmapss_gold import build_dim_engine


@pytest.fixture(scope="session")
# def spark():
#     """Provides a local Spark session for unit testing transforms."""
#     return SparkSession.builder \
#         .appName("CmapssGoldUnitTest") \
#         .master("local[2]") \
#         .config("spark.ui.enabled", "false") \
#         .config("spark.driver.host", "127.0.0.1") \
#         .config("spark.driver.bindAddress", "127.0.0.1") \
#         .getOrCreate()

def test_build_dim_engine_valid_records (spark):
    schema = StructType([
        StructField("engine_id", IntegerType(), True),
        StructField("max_cycle", IntegerType(), True),
        StructField("cycle", IntegerType(), True),
        StructField("setting_1", FloatType(), True),

    ])

    result_schema = StructType([
        StructField("engine_id", IntegerType(), True),
        StructField("total_lifespan_cycles", IntegerType(), True),
        StructField("first_recorded_cycle", IntegerType(), True),
        StructField("health_status_category", StringType(), True),

    ])
    data1 = [(1,190,1,1.1),(1,190,1,1.2)]

    df_data1 = spark.createDataFrame(data1, schema)
    df_result_data1 = build_dim_engine(df_data1)
   

    mocked_data_data1 = [(1,190,1, "Standard Wear")]
    expected_data1_df = spark.createDataFrame(mocked_data_data1, result_schema)

    # print("Actual data schema")
    # df_result.printSchema()
    # print("mocked data schema")
    # expected_df.printSchema()
    results = expected_data1_df.collect()
    assert len(results) == 1
    assertDataFrameEqual(df_result_data1, expected_data1_df)

    data2 = [(1,10,1,1.1),(1,10,1,1.2)]
    df_data2 = spark.createDataFrame(data2, schema)
    df_result_data2 = build_dim_engine(df_data2)

    mocked_data_data2 = [(1, 10, 1, "Early Failure")]
    assertDataFrameEqual(df_result_data2, mocked_data_data2)
