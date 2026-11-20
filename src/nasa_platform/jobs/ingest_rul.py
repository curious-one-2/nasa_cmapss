from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, lit
from nasa_platform.core.registry import register_job
from nasa_platform.jobs.ingest_archive_cmapss import move_ingested_source_file
from nasa_platform.core.job_base import JobBase
from pathlib import Path
import shutil

@register_job("ingest_rul")
class IngestRULJob(JobBase):
    def execute(self) -> str:
     # Only 1 field
        fields = [
            StructField("rul", IntegerType(), False),
        ]

        schema= StructType(fields)

        input_path = self.config.get("input_path","data/landing/rul")
        output_path = self.config.get("output_path", "data/lakehouse/bronze/rul")
        landing_dir = Path("data/landing/rul")
        archive_dir = Path("data/archive/rul")    

        # checking if source files are available and there is only 1 source file.
        source_file = list(landing_dir.glob("*.txt"))

        if not source_file:
            self.logger.info(f"-> [SKIPPED] No source files found in landing directory: {landing_dir}")
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
        archived_file_path = archive_dir / source_file_path.name
        source_file_str = str(source_file_path)
        archived_file_str = str(archive_dir / source_file_path.name)
        #archived_file_path = Path(archived_file_str)

        # Check if this file has already been processed and archived
        if archived_file_path.exists():
            self.logger.info(
                f"-> [SKIPPED] Source file '{source_file_path.name}' has already been processed "
                f"and archived at '{archived_file_path}'. Exiting gracefully."
            )
            return "ALREADY_PROCESSED"
        
        self.logger.info(f"Reading raw RUL data from {source_file_path}")
        

        # Read space delimited text file
        df = (
            self.spark.read
            .option("sep", " ")
            .option("header", "false")
            .schema(schema)
            .csv(str(source_file_str))
        )

        # Drop trailing null columns if present from extra spaces
        cleaned_cols = [c for c in df.columns if c is not None]
        df_selected = df.select(cleaned_cols)

        # Add Bronze audit columns
        bronze_df = (
            df_selected
            .withColumn("ingest_timestamp", current_timestamp())
            .withColumn("source_file", lit(source_file_str))
        )
        self.logger.info(f"Writing Rul Bronze Delta table to {output_path}...")
        #print(f"Writing Bronze Delta table to {output_path}...")
        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .save(output_path)
        )
        #self.logger.info("Bronze C-MAPSS ingestion complete.")
        print("Bronze RUL ingestion complete.")

        archive_result = move_ingested_source_file(str(source_file_path), str(archived_file_path))
        # 4. Archive the source file
        #shutil.move(str(source_file_path), str(archived_file_path))
        return_value =""
        if archive_result == "ARCHIVE_SUCCESS":

            self.logger.info(
                f"-> Bronze RUL ingestion complete for {source_file_path.name}. "
                f"Archived source file to: {archived_file_path}"
            )
            return_value="SUCCESS"
        else:
            self.logger.error(f"Failed to archive the file {source_file_path.name}")
            return_value= archive_result

        return return_value
