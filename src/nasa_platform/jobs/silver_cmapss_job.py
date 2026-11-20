from delta.tables import DeltaTable
from nasa_platform.core.job_base import JobBase
from nasa_platform.core.registry import register_job
from nasa_platform.transforms.silver.cmapss_silver import clean_and_validate_cmapss

@register_job("silver_cmapss")
class SilverCmapssJob(JobBase):
    def execute(self) -> None:
        # bronze_path = self.config.get("bronze_path", "data/lakehouse/bronze/cmapss_sensors")
        # silver_path = self.config.get("silver_path", "data/lakehouse/silver/cmapss_sensors")
        # quarantine_path = self.config.get("quarantine_path", "data/lakehouse/quarantine/cmapss_sensors")
        is_test = self.config.get("test_run", {}).get("is_test_run", False)
        
        # Dynamically route paths based on whether this is a train or test run
        if is_test:
            bronze_path = self.config.get("bronze_path_test", "data/lakehouse/bronze/cmapss_sensors_test")
            silver_path = self.config.get("silver_path_test", "data/lakehouse/silver/cmapss_sensors_test")
            quarantine_path = self.config.get("quarantine_path_test", "data/lakehouse/quarantine/cmapss_sensors_test")
            self.logger.info("-> Running Silver C-MAPSS Pipeline for TEST dataset")
        else:
            bronze_path = self.config.get("bronze_path", "data/lakehouse/bronze/cmapss_sensors_train")
            silver_path = self.config.get("silver_path", "data/lakehouse/silver/cmapss_sensors")
            quarantine_path = self.config.get("quarantine_path", "data/lakehouse/quarantine/cmapss_sensors")
            self.logger.info("-> Running Silver C-MAPSS Pipeline for TRAINING dataset")


        self.logger.info(f"Reading Bronze Delta table from {bronze_path}")
        bronze_df = self.spark.read.format("delta").load(bronze_path)

        self.logger.info("Applying Silver cleaning and validation rules...")
        processed_df = clean_and_validate_cmapss(bronze_df)

        # Cache dataframe since we split it into two sinks (Silver and Quarantine)
        processed_df.cache()

        # Separate valid vs invalid records
        silver_df = processed_df.filter("is_valid == true").drop("is_valid", "validation_error")
        quarantine_df = processed_df.filter("is_valid == false")

        valid_count = silver_df.count()
        quarantine_count = quarantine_df.count()
        self.logger.info(f"Validation summary: {valid_count} valid records, {quarantine_count} quarantined records.")

        # Write Silver Layer (Merge/Append Delta)
        self.logger.info(f"Writing valid records to Silver Delta path: {silver_path}")
        silver_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(silver_path)

        # Write Quarantine Layer if any bad records exist
        if quarantine_count > 0:
            self.logger.warning(f"Writing invalid records to Quarantine path: {quarantine_path}")
            quarantine_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(quarantine_path)

        processed_df.unpersist()
        self.logger.info("Silver C-MAPSS job completed successfully.")