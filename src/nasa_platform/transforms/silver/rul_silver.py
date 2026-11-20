
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window
def transform_silver_rul(bronze_df: DataFrame):
    """
    Parses raw single-column RUL text into a structured table with engine_id and rul.
    """
    # Dynamically grab the first column name (avoiding hardcoded 'value')
    raw_col = bronze_df.columns[0]

    # Assign sequential engine IDs based on row order in the test ground truth file
    w = Window.orderBy(F.monotonically_increasing_id())
    processing_silver_df = bronze_df.withColumn("rul", F.col(raw_col).cast("Integer")).withColumn(
        "engine_id", F.row_number().over(w)
        ).select("engine_id", "rul")
    
    return processing_silver_df
