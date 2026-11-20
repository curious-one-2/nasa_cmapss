from delta.tables import DeltaTable
from nasa_platform.core.job_base import JobBase
from nasa_platform.core.registry import register_job
from nasa_platform.transforms.gold.cmapss_gold import transform_gold_features_and_rul, build_fact_engine_telemetry, build_dim_engine

@register_job("gold_cmapss")
class SilverCmapssJob(JobBase):

    def write_gold_layer(spark, featured_df, lakehouse_base_path):
        fact_df = build_fact_engine_telemetry(featured_df)
        dim_df = build_dim_engine(featured_df)

        # Write Fact Table
        (
            fact_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{lakehouse_base_path}/gold/fact_engine_telemetry")
        )

        # Write Dimension Table
        (
            dim_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(f"{lakehouse_base_path}/gold/dim_engine")
        )

    def execute(self) -> None:
       # bronze_path = self.config.get("bronze_path", "data/lakehouse/bronze/cmapss_sensors")
        silver_path = self.config.get("silver_path", "data/lakehouse/silver/cmapss_sensors")
        gold_path = self.config.get("gold_path", "data/lakehouse/gold/cmapss_sensors")
        lakehouse_base_path = self.config.get("gold_path", "data/lakehouse")
        self.logger.info("Reading Silver data ...")
        silver_df = self.spark.read.format("delta").load(silver_path)
        processed_gold_df = transform_gold_features_and_rul(silver_df)
        self.logger.info("Processing Gold data ...")
        self.write_gold_layer(processed_gold_df, lakehouse_base_path)
        self.logger.info("Gold C-MAPSS job completed successfully.")