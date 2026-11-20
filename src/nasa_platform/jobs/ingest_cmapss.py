from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, lit
from nasa_platform.core.registry import register_job
from nasa_platform.jobs.ingest_archive_cmapss import move_ingested_source_file
from nasa_platform.core.job_base import JobBase
from pathlib import Path
import shutil

@register_job("ingest_cmapss")
class IngestCMAPSSJob(JobBase):
    def execute(self) -> str:
     # C-MAPSS space-separated schema: Unit, Cycle, 3 operational settings, 21 sensor measurements
        fields = [
            StructField("unit_id", IntegerType(), False),
            StructField("time_cycle", IntegerType(), False),
            StructField("setting_1", FloatType(), True),
            StructField("setting_2", FloatType(), True),
            StructField("setting_3", FloatType(), True),
        ]
        # Add 21 sensor columns
        for i in range(1, 22):
            fields.append(StructField(f"sensor_{i}", FloatType(), True))

        schema= StructType(fields)

        # input_path = self.config.get("input_path","data/landing/cmapss")
        # output_path = self.config.get("output_path", "data/lakehouse/bronze/cmapss_sensors")
        landing_dir = Path("data/landing/cmapss")
        # archive_dir = Path("data/archive/cmapss")    

        # Distinguish test files (starting with 'test_') from training files
        all_files = list(landing_dir.glob("*.txt"))
        
        # Filter for test files if specified or by prefix convention
        is_test_run = self.config.get("test_run", {}).get("is_test_run", False)
        source_file = [
            f for f in all_files 
            if (f.name.startswith("test_") if is_test_run else not f.name.startswith("test_"))
        ]
        if not source_file:
            self.logger.info(f"-> [SKIPPED] No matching C-MAPSS {'test' if is_test_run else 'train'} files found in {landing_dir}")
            return "NO_SOURCE_FOUND"

        if len(source_file) > 1:
            file_names=[f.name for f in source_file]
            error_msg = (
                f"-> [ERROR] Multiple source files detected in landing directory ({len(source_file)} found: {file_names}). "
                f"Expected exactly 1 file to prevent multi-batch collision. Failing job."
            )
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 3. Process the single validated file
        source_file_path = source_file[0]
        dataset_type = "test" if is_test_run else "train"
        archive_dir = Path(f"data/archive/cmapss/{dataset_type}")
        archived_file_path = archive_dir / source_file_path.name
        #archived_file_path = archive_dir / source_file_path.name
        #source_file_str = str(source_file_path)
        archived_file_str = str(archive_dir / source_file_path.name)
        #archived_file_path = Path(archived_file_str)

        # Check if this file has already been processed and archived
        if archived_file_path.exists():
            self.logger.info(
                f"-> [SKIPPED] Source file '{source_file_path.name}' has already been processed "
                f"and archived at '{archived_file_path}'. Exiting gracefully."
            )
            return "ALREADY_PROCESSED"

        output_path = self.config.get(
            "bronze_cmapss_test" if is_test_run else "bronze_cmapss", 
            f"data/lakehouse/bronze/cmapss_sensors_{dataset_type}"
        )


        self.logger.info(f"Reading raw C-MAPSS data from {source_file_path}")
        #print(f"Reading raw C-MAPSS data from {input_path}")

        # Read space delimited text file
        df = (
            self.spark.read
            .option("sep", " ")
            .option("header", "false")
            .schema(schema)
            .csv(str(source_file_path))
        )

        # Drop trailing null columns if present from extra spaces
        cleaned_cols = [c for c in df.columns if c is not None]
        df_selected = df.select(cleaned_cols)

        # Add Bronze audit columns
        bronze_df = (
            df_selected
            .withColumn("ingest_timestamp", current_timestamp())
            .withColumn("source_file", lit(str(source_file_path)))
        )
        self.logger.info(f"Writing Bronze Delta table to {output_path}...")
        #print(f"Writing Bronze Delta table to {output_path}...")
        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .save(output_path)
        )
        #self.logger.info("Bronze C-MAPSS ingestion complete.")
        print(f"Bronze C-MAPSS {dataset_type} ingestion complete -> {output_path}")
        print(f"source_file_path: {source_file_path}, archived_file_path {archived_file_path}")

        archive_result = move_ingested_source_file(str(source_file_path), str(archived_file_path))
        # 4. Archive the source file
        #shutil.move(str(source_file_path), str(archived_file_path))
        return_value =""
        if archive_result == "ARCHIVE_SUCCESS":

            self.logger.info(
                f"-> Bronze C-MAPSS ingestion complete for {source_file_path.name}. "
                f"Archived source file to: {archived_file_path}"
            )
            return_value="SUCCESS"
        else:
            self.logger.error(f"Failed to archive the file {source_file_path.name}")
            return_value= archive_result

        return return_value
