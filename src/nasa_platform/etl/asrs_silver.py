from pyspark.sql.functions import col, trim
from nasa_platform.core.registry import register_job
from nasa_platform.core.job_base import JobBase


@register_job("transform_asrs_silver")
class TransformASRSSilverJob(JobBase):
    def execute(self) -> None:
        input_path = self.config.get("input_path", "data/lakehouse/bronze/asrs")
        output_path = self.config.get("output_path", "data/lakehouse/silver/asrs")

        self.logger.info(f"Reading ASRS Bronze Delta table from {input_path}...")
        #print(f"Reading ASRS Bronze Delta table from {input_path}...")
        bronze_df = self.spark.read.format("delta").load(input_path)

        # Clean text, drop nulls, remove duplicates
        silver_df = (
            bronze_df
            .select(
                col("incident_id").cast("string"),
                trim(col("narrative")).alias("narrative"),
                col("ingested_at")
            )
            .filter(col("incident_id").isNotNull() & col("narrative").isNotNull())
            .dropDuplicates(["incident_id"])
        )
        self.logger.info(f"Writing Silver Delta table to {output_path}...")
        #print(f"Writing Silver Delta table to {output_path}...")
        (
            silver_df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(output_path)
        )
        self.logger.info("Silver ASRS transformation complete.")
        #print("Silver ASRS transformation complete.")