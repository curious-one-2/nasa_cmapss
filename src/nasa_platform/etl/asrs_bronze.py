from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from nasa_platform.core.registry import register_job
from nasa_platform.core.job_base import JobBase
from nasa_platform.jobs.ingest_archive_cmapss import move_ingested_source_file
from pathlib import Path
import glob 

@register_job("ingest_asrs")
class IngestASRSJob(JobBase):
    def execute(self) -> str:

        src_schema = StructType([
            StructField("incident_id", StringType(), True),
            StructField("narrative", StringType(), True),
        ])

        input_path = Path("data/landing/asrs")
        output_path = Path("data/lakehouse/bronze/asrs")
        archive_path = Path("data/archive/asrs")    

        # checking if source files are available and there is only 1 source file.
        source_file = list(input_path.glob("*.json"))

        if not source_file:
            self.logger.info(f"-> [SKIPPED] No ASRS source files found in landing directory: {input_path}")
            return "NO_SOURCE_FOUND"

        if len(source_file) > 1:
            file_names=[f.name for f in source_file]
            error_msg = (
                f"-> [ERROR] Multiple ASRS source files detected in landing directory ({len(source_file)} found: {file_names}). "
                f"Expected exactly 1 file to prevent multi-batch collision. Failing job."
            )
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # 3. Process the single validated file
        source_file_path = source_file[0]
        archived_file_path = archive_path  /  source_file_path
        source_file_str = str(source_file_path)
        #archived_file_str = str(archive_path / source_file_path)
        #archived_file_path = Path(archived_file_str)

        # Check if this file has already been processed and archived
        if archived_file_path.exists():
            self.logger.info(
                f"-> [SKIPPED] Source file '{source_file_path.name}' has already been processed "
                f"and archived at '{archived_file_path}'."
            )
            return "ALREADY_PROCESSED"
        

        self.logger.info(f"Reading raw sample narratives data from {input_path}")
        #print(f"Reading raw sample narratives data from {input_path}")

        # Read space delimited text file

        raw_df = (
            self.spark.read
            .schema(src_schema)
            .option("multiLine", "true")
            .json(str(input_path))
        )

        # Add additional fields
        bronze_df = raw_df \
                .withColumn("ingested_at", current_timestamp()) \
                .withColumn("source_file", input_file_name())
        
        self.logger.info(f"Writing Bronze Delta table to {output_path}...")
        #print(f"Writing Bronze Delta table to {output_path}...")
        (
            bronze_df.write
            .format("delta")
            .mode("append")
            .save(str(output_path))
        )
        print(f"before archive: {source_file_path} {archive_path}")
        archive_result = move_ingested_source_file(str(source_file_path), str(archive_path))
        # 4. Archive the source file
        return_value =""
        if archive_result == "ARCHIVE_SUCCESS":

            self.logger.info(
                f"-> Bronze sample narratives ingestion complete {source_file_path.name}. "
                f"Archived ASRS source file to: {archive_path}"
            )
            return_value="SUCCESS"
        else:
            self.logger.error(f"Failed to archive the file {source_file_path.name}")
            return_value= archive_result
        return return_value