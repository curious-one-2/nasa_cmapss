from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def clean_and_validate_cmapss(df: DataFrame) -> DataFrame:
    """
    Cleans Bronze C-MAPSS sensor data, checks for structural validity,
    deduplicates, and appends metadata columns for downstream quarantine routing.
    
    Validation Rules:
    - unit_id and time_cycle must not be null and must be > 0
    - No duplicate (unit_id, time_cycle) readings
    """
    # 1. Deduplicate on natural primary key
    deduped_df = df.dropDuplicates(["unit_id", "time_cycle"])

    # 2. Add validation expressions
    validated_df = deduped_df.withColumn(
        "validation_error",
        F.when(F.col("unit_id").isNull() | (F.col("unit_id") <= 0), "Invalid or missing unit_id")
         .when(F.col("time_cycle").isNull() | (F.col("time_cycle") <= 0), "Invalid or missing time_cycle")
         .otherwise(F.lit(None))
    ).withColumn(
        "is_valid",
        F.col("validation_error").isNull()
    ).withColumn(
        "processed_at",
        F.current_timestamp()
    )

    return validated_df