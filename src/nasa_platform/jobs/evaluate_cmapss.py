"""Evaluation Job: Scores test set RUL predictions against NASA C-MAPSS
ground-truth RUL table from the Silver layer, computing RMSE and the PHM asymmetric scoring metric.
"""

from pathlib import Path
import numpy as np
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
from nasa_platform.core.registry import register_job
from nasa_platform.core.job_base import JobBase


@register_job("evaluate_cmapss")
class EvaluateCMAPSSJob(JobBase):

  def execute(self) -> dict:
    self.logger.info("-> Starting C-MAPSS Test Evaluation Job...")

    test_features_path = self.config.get("paths", {}).get(
        "gold_telemetry_test", "data/lakehouse/gold/fact_engine_telemetry_test"
    )
    silver_rul_path = self.config.get("paths", {}).get(
        "silver_rul", "data/lakehouse/silver/rul"
    )

    if not Path(test_features_path).exists():
      raise FileNotFoundError(f"Test feature Delta table not found at {test_features_path}.")
    if not Path(silver_rul_path).exists():
      raise FileNotFoundError(f"Silver RUL Delta table not found at {silver_rul_path}. Run silver RUL pipeline first.")

    # 1. Read test features
    df_test = self.spark.read.format("delta").load(test_features_path)
    
    # Use model predictions if available; otherwise use gold table's rul for plumbing validation
    #pred_col = "predicted_rul" if "predicted_rul" in df_test.columns else "rul"
    pred_col = "rul"
    window_spec = Window.partitionBy("engine_id").orderBy(F.col("cycle").desc())

    df_final = (
        df_test.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .select(
            F.col("engine_id"),
            F.col("cycle").alias("max_cycle"),
            F.col(pred_col).alias("predicted_rul"),
        )
    )

    # 2. Read ground truth RUL values from the Silver Delta table
    df_gt = self.spark.read.format("delta").load(silver_rul_path)

    # Ensure engine_id types match for a clean join
    df_final = df_final.withColumn("engine_id", F.col("engine_id").cast("integer"))
    df_gt = df_gt.withColumn("engine_id", F.col("engine_id").cast("integer"))

    # 3. Join predictions with ground truth on engine_id and drop any rows with nulls
    df_eval = (
        df_final.join(df_gt, on="engine_id", how="inner")
        .select(
            F.col("predicted_rul").cast("float"),
            F.col("rul").cast("float")
        )
        .na.drop() # Drop rows where either prediction or ground truth is null
    )
    
    eval_rows = df_eval.collect()

    if not eval_rows:
        raise ValueError(
          "Evaluation set resulted in zero matched rows between predictions and ground truth. "
          "Check that engine_id values and types match between fact_engine_telemetry_test and silver_rul."
        )
    preds = np.array([r["predicted_rul"] for r in eval_rows])
    actuals = np.array([r["rul"] for r in eval_rows])

    # 4. Compute metrics
    rmse = float(np.sqrt(np.mean((preds - actuals) ** 2)))
    
    # PHM Asymmetric Score calculation
    d = preds - actuals
    phm_score = float(np.sum(np.where(d < 0, np.exp(-d / 13.0) - 1.0, np.exp(d / 10.0) - 1.0)))

    self.logger.info(f"-> Evaluation Results -> RMSE: {rmse:.4f} | PHM Score: {phm_score:.2f}")

    return {
        "status": "SUCCESS",
        "rmse": rmse,
        "phm_score": phm_score,
        "units_evaluated": len(eval_rows),
    }