from pathlib import Path
from nasa_platform.core.job_base import JobBase
from nasa_platform.core.registry import register_job
from nasa_platform.transforms.gold.cmapss_gold import transform_gold_features_and_rul, build_fact_engine_telemetry

@register_job("gold_cmapss_test")
class GoldCMAPSSTestJob(JobBase):

    def execute(self) -> None:
        self.logger.info("-> Starting Gold C-MAPSS Test Pipeline ...")
        silver_test_path= self.config.get("paths", {}).get("silver_cmapss_test", "data/lakehouse/silver/cmapss_sensors_test")
        silver_rul_path=self.config.get("paths", {}).get("silver_rul", "data/lakehouse/silver/rul")
        gold_test_path=self.config.get("paths", {}).get("gold_telemetry_test", "data/lakehouse/gold/fact_engine_telemetry_test")

        if not Path(silver_test_path).exists():
            raise FileNotFoundError(f"Silver test sensors path not found at {silver_test_path}.")
        if not Path(silver_rul_path).exists():
            raise FileNotFoundError(f"Silver RUL path not found at {silver_rul_path}.")

        df_silver_test=self.spark.read.format("delta").load(silver_test_path)
        df_rul=self.spark.read.format("delta").load(silver_rul_path)

        self.logger.info("-> Applying test-set RUL reconstruction and feature engineering...")
        featured_test_df = transform_gold_features_and_rul(df_silver_test, is_test_set=True, rul_ground_truth_df=df_rul)

        fact_test_df=build_fact_engine_telemetry(featured_test_df)

        self.logger.info(f"-> Writing test telemetry to Gold path: {gold_test_path}")
        (
            fact_test_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(gold_test_path)
        )

        self.logger.info("-> Gold C-MAPSS Test Job completed successfully.")