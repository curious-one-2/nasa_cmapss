"""Prefect Orchestration Workflow for the Dual-Track NASA Lakehouse & RAG
Platform.

utilization:
--mode <mode parameter> --is_test_run <true, false' (default)>
"""
import argparse
import yaml
from pathlib import Path
from prefect import flow, task
from nasa_platform.core.spark_session import LocalSparkFactory
from nasa_platform.core.registry import JOB_REGISTRY

from nasa_platform.core.logger import setup_logging

# Ensure all jobs are registered by importing them
import nasa_platform.jobs.ingest_cmapss
import nasa_platform.jobs.silver_cmapss_job
import nasa_platform.jobs.gold_cmapss_job
import nasa_platform.etl.asrs_bronze
import nasa_platform.etl.asrs_silver
import nasa_platform.rag.vector_ingest
import nasa_platform.jobs.hybrid_diagnostics
import nasa_platform.jobs.ingest_rul
import nasa_platform.jobs.silver_rul_job
import nasa_platform.jobs.evaluate_cmapss
import nasa_platform.jobs.gold_cmapss_test
import nasa_platform.ml.train_and_inference

setup_logging()

@task(name="Load Configuration", retries=2)
def load_config(env: str = "dev") -> dict:
  config_path = Path(f"conf/app/{env}.yaml")
  if not config_path.exists():
    raise FileNotFoundError(f"Configuration file not found at {config_path}")
  with open(config_path, "r") as f:
    return yaml.safe_load(f)


@task(name="Initialize Spark Session")
def init_spark(cfg: dict):
  spark_cfg = cfg.get("spark", {})
  return (
      LocalSparkFactory.get_session(
          app_name="NASA-Prefect-Pipeline",
          master=spark_cfg.get("master", "local[4]"),
      #    shuffle_partitions=str(spark_cfg.get("shuffle.partitions", "8")),
      #    driver_memory=spark_cfg.get("driver.memory", "4g"),
      )
  )


@task(name="Execute Job Task", cache_policy=None)
def run_job_task(job_name: str, spark, config: dict):
  if job_name not in JOB_REGISTRY:
    raise KeyError(f"Job '{job_name}' is not registered in JOB_REGISTRY.")

  job_class = JOB_REGISTRY[job_name]
  job_instance = job_class(spark=spark, config=config)
  result = job_instance.run()
  #return f"Successfully completed job: {job_name}"
  print(f"result: {result}")
  return result if result is not None else f"Successfully completed job: {job_name}"

@flow(name="NASA Lakehouse AND RAG End-to-End Pipeline", log_prints=True)
def nasa_pipeline_flow(env: str = "dev", mode: str="all", is_test_run: bool=None ):
  print(f"-> Starting NASA Data Platform Pipeline (Environment: {env} and mode {mode})")

  # 1. Load config and start Spark
  cfg = load_config(env)
 # Safely convert the incoming argument to a true Python boolean
  if is_test_run is not None:
      # If it's already a boolean (from argparse lambda), keep it; if string, parse it
      if isinstance(is_test_run, bool):
          bool_test_run = is_test_run
      else:
          bool_test_run = str(is_test_run).strip().lower() == 'true'
          
      cfg["test_run"]["is_test_run"] = bool_test_run

  spark = init_spark(cfg)

  try:
    if mode in ["all", "cmapss"]:
        # 2. Execute Structured Track (C-MAPSS Medallion Pipeline)
        print("\n=== STAGE 1: Structured Track (C-MAPSS Medallion) ====")
        cmapss_ingest_status = run_job_task("ingest_cmapss", spark, cfg)
        print(f"-> C-MAPSS Ingestion Status: {cmapss_ingest_status}")

        if cmapss_ingest_status == "NO_SOURCE_FOUND":
            print("-> Pipeline halted: No C-MAPSS source files found in landing zone.")
            return
        elif cmapss_ingest_status == "ALREADY_PROCESSED":
            print("-> Pipeline halted: C-MAPSS Source file has already been processed and archived.")
            return
        elif cmapss_ingest_status == "MULTIPLE_SOURCES_FOUND":
            # Handled via ValueError inside task, but added as defensive check here
            print("-> Pipeline failed: Multiple C-MAPSS source files detected.")
            raise ValueError("Multiple C-MAPSS source files found in landing directory.")
        elif cmapss_ingest_status == "ARCHIVE_ERROR":
            print("-> Pipeline failed: Failed to archive the C-MAPSS source file.")
            raise ValueError("Failed to archive the C-MAPSS source file.")
      
    
        run_job_task("silver_cmapss", spark, cfg)
        if is_test_run is None or not(is_test_run):
            run_job_task("gold_cmapss", spark, cfg)
        elif is_test_run:
            run_job_task("gold_cmapss_test", spark, cfg)

            # 5. Train model and run batch inference to populate predicted_rul
            run_job_task("train_and_inference_cmapss", spark, cfg)
            # 6. Evaluate actual predictions against ground truth

            # 4. Evaluate predictions against ground truth
            eval_result =run_job_task("evaluate_cmapss", spark, cfg)

            print(f"\n[EVALUATION METRICS REPORT]")
            print(f"-> Status: {eval_result.get('status')}")
            print(f"-> RMSE: {eval_result.get('rmse'):.4f}")
            print(f"-> PHM Score: {eval_result.get('phm_score'):.2f}")
            print(f"-> Units Evaluated: {eval_result.get('units_evaluated')}")

    # 3. Execute Unstructured Track (ASRS RAG & Vector Ingestion)
    # Track 2: Unstructured ASRS RAG & Vector Ingestion
    if mode in ["all", "asrs"]:
        print("\n=== STAGE 2: Unstructured Track (ASRS Vector Store) ====")

        asrs_ingest_status = run_job_task("ingest_asrs", spark, cfg)
        print(f"-> ASRS Ingestion Status: {asrs_ingest_status}")

        if asrs_ingest_status == "NO_SOURCE_FOUND":
            print("-> No new ASRS source file to process. Continuing...")
        elif asrs_ingest_status == "ALREADY_PROCESSED":
            print("-> Pipeline halted: Source file has already been processed and archived.  Continuing ...")
        elif asrs_ingest_status == "MULTIPLE_SOURCES_FOUND":
            # Handled via ValueError inside task, but added as defensive check here
            print("-> Pipeline failed: Multiple ASRS source files detected.")
            raise ValueError("Multiple ASRS source files found in landing directory.")
        elif asrs_ingest_status == "ARCHIVE_ERROR":
            print("-> Pipeline failed: Failed to archive the ASRS source file.")
            raise ValueError("Failed to archive the ASRS source file.")

        if asrs_ingest_status=="SUCCESS":
            run_job_task("transform_asrs_silver", spark, cfg)
            run_job_task("ingest_asrs_vectors", spark, cfg)

   # Track 3: RUL file Ingestion
    if mode in ["all", "rul"]:
        print("\n=== STAGE 3: Structured Track (RUL Medallion) ====")

        rul_ingest_status = run_job_task("ingest_rul", spark, cfg)
        print(f"-> RUL Ingestion Status: {rul_ingest_status}")

        if rul_ingest_status == "NO_SOURCE_FOUND":
            print("-> No new RUL source file to process. Continuing...")
        elif rul_ingest_status == "ALREADY_PROCESSED":
            print("-> Pipeline halted: RUL Source file has already been processed and archived.  Continuing ...")
        elif rul_ingest_status == "MULTIPLE_SOURCES_FOUND":
            # Handled via ValueError inside task, but added as defensive check here
            print("-> Pipeline failed: Multiple RUL source files detected.")
            raise ValueError("Multiple RUL source files found in landing directory.")
        elif rul_ingest_status == "ARCHIVE_ERROR":
            print("-> Pipeline failed: Failed to archive the RUL source file.")
            raise ValueError("Failed to archive the RUL source file.")

        if rul_ingest_status=="SUCCESS":
            run_job_task("silver_rul", spark, cfg)

    # 4. Execute Hybrid Diagnostics Bridge
    if mode in ["all", "hybrid"]:
        print("\n=== STAGE 3: Hybrid Diagnostics Bridge ====")
        run_job_task("hybrid_diagnostics", spark, cfg)

    print(
        "\n-> End-to-End NASA Lakehouse  RAG Pipeline completed successfully!"
    )

  finally:
    spark.stop()
    print("-> SparkSession stopped.")


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="NASA Lakehouse AND RAG Pipeline Orchestrator")
  parser.add_argument(
      "--mode",
      type=str,
      default="all",
      choices=["all", "cmapss", "asrs","hybrid", "rul"],
      help="Pipeline execution mode: 'all' (default), 'cmapss', 'asrs', 'hybrid', 'rul'."
  )
  parser.add_argument(
      "--env",
      type=str,
      default="dev",
      help="Environment configuration file name (default: dev)."
  )
  parser.add_argument(
     "--is_test_run",
     type=lambda x: str(x).strip().lower() == 'true',
     default="False",
     help="Flag to indicate whether to process C-MAPSS test dataset instead of training data."
  )
  args = parser.parse_args()

  nasa_pipeline_flow(env=args.env, mode=args.mode, is_test_run=args.is_test_run)