"""
Hybrid Bridge Job: Correlates C-MAPSS Gold telemetry anomalies with
ChromaDB ASRS semantic search for automated root-cause diagnostics.
"""

import logging
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from nasa_platform.core.registry import register_job
from nasa_platform.core.job_base import JobBase


@register_job("hybrid_diagnostics")
class HybridDiagnosticsJob(JobBase):

  def execute(self) -> None:
    self.logger.info("-> Starting Hybrid Diagnostics Pipeline...")

    # Use self.config passed by JobBase/Prefect and check if this is a test run
    is_test = self.config.get("test_run", {}).get("is_test_run", False)
    paths_cfg = self.config.get("paths", {})
    vector_cfg = self.config.get("vector_store", {})

    if is_test:
      gold_path = paths_cfg.get("gold_telemetry_test", "data/lakehouse/gold/fact_engine_telemetry_test")
      self.logger.info("-> Mode: TEST run detected. Using test Gold telemetry.")
    else:
      gold_path = paths_cfg.get("gold_telemetry", "data/lakehouse/gold/fact_engine_telemetry")
      self.logger.info("-> Mode: TRAINING run detected. Using training Gold telemetry.")

    chroma_persist_dir = paths_cfg.get("vector_store", "data/vector_store")
    collection_name = vector_cfg.get("collection_name", "nasa_asrs_incidents")

    # 1. Load telemetry data from Gold layer with strict error propagation
    self.logger.info(f"-> Reading Gold telemetry from: {gold_path}")
    if not Path(gold_path).exists():
      raise FileNotFoundError(f"Gold telemetry table path does not exist: {gold_path}. Run the C-MAPSS pipeline first.")
    
    telemetry_df = self.spark.read.format("delta").load(gold_path)
    if telemetry_df.count() == 0:
      raise ValueError(f"Gold telemetry table at {gold_path} is empty.")

    # 2. Dynamic multi-sensor anomaly filtering
    cols = telemetry_df.columns
    #sensor_col = "sensor_2" if "sensor_2" in cols else ("s_2" if "s_2" in cols else None)
    sensor_col =["sensor_2", "sensor_3", "sensor_4", "sensor_11", "sensor_12", "sensor_15"]
    # if sensor_col:
    #   self.logger.info(f"-> Applying dynamic anomaly filters based on {sensor_col} and rolling features...")
    #   anomalies_df = telemetry_df.filter(
    #       (F.col(sensor_col) > 640.0) | 
    #       (F.col("rul") < 20) # Catch engines nearing critical failure threshold
    #   )
    # combined_condition = F.lit(False)
    # if "rul" in cols:
    #   combined_condition = combined_condition | (F.col("rul") < 20)
    # if "predicted_rul" in cols:
    #   combined_condition = combined_condition | (F.col("predicted_rul") < 30)

    # for s in sensor_col:
    #   std_col = f"{s}_roll_std_5"
    #   trend_col = f"{s}_trend_5"
    #   if std_col in cols:
    #     s_cond = F.col(std_col) > 1.5
    #     if trend_col in cols:
    #       s_cond = s_cond | (F.col(trend_col) > 2.0)
    #     combined_condition = combined_condition | s_cond
    #   else:
    #     self.logger.warning(
    #         f"-> '{std_col}' not found in schema; falling back to {s} > 640.0."
    #     )
    #     combined_condition = combined_condition | (F.col(s) > 640.0)

    # self.logger.info(
    #     "-> Applying combined anomaly filters across sensors:"
    #     f" {sensor_col}..."
    # )
    sensor_thresholds = {
    2: {"std": 0.48, "trend": 0.50},
    3: {"std": 6.80, "trend": 7.50},
    4: {"std": 6.70, "trend": 7.40},
    11: {"std": 0.16, "trend": 0.17},
    12: {"std": 0.49, "trend": 0.48},
    15: {"std": 0.032, "trend": 0.033},
    }

    # Validate required columns and keys fail-fast before building expressions
    required_cols = {"rul", "predicted_rul"}
    for sensor_id in sensor_thresholds:
      required_cols.add(f"sensor_{sensor_id}_roll_std_5")
      required_cols.add(f"sensor_{sensor_id}_trend_5")

    missing_cols = required_cols - set(telemetry_df.columns)
    if missing_cols:
      raise ValueError(f"Missing required columns in df_test: {sorted(missing_cols)}")

    combined_condition = (
        F.coalesce(F.col("rul") < 20, F.lit(False))
        | F.coalesce(F.col("predicted_rul") < 30, F.lit(False))
    )

    for sensor_id, thresholds in sensor_thresholds.items():
      if "std" not in thresholds or "trend" not in thresholds:
        raise KeyError(
            f"Missing 'std' or 'trend' threshold for sensor_{sensor_id}: {thresholds}"
        )

      std_col = F.col(f"sensor_{sensor_id}_roll_std_5")
      trend_col = F.col(f"sensor_{sensor_id}_trend_5")

      # Use F.abs(trend_col) if both sharp upward spikes and sharp drops should trigger
      sensor_cond = (std_col > F.lit(thresholds["std"])) | (
          F.abs(trend_col) > F.lit(thresholds["trend"])
      )

      combined_condition = combined_condition | F.coalesce(sensor_cond, F.lit(False))


    anomalies_df = telemetry_df.filter(combined_condition)
    anomaly_rows = anomalies_df.collect()
      #else:
      #  raise ValueError("Required sensor columns not found in Gold telemetry dataframe schema.")

    #anomaly_rows = anomalies_df.limit(10).collect()
    #anomaly_rows = anomalies_df.collect()

    if not anomaly_rows:
      self.logger.info("-> No critical anomalies detected in Gold telemetry.")
      return

    self.logger.info(
        f"-> Found {len(anomaly_rows)} engine anomaly records. Querying"
        " ChromaDB for root-cause matches..."
    )

    # 3. Import ChromaDB and Ollama locally, ensuring collection exists safely
    import chromadb
    import ollama

    client = chromadb.PersistentClient(path=chroma_persist_dir)
    
    try:
      collection = client.get_or_create_collection(name=collection_name)
      self.logger.info(f"-> Successfully loaded/initialized Chroma collection: '{collection_name}'")
    except Exception as e:
      self.logger.error(
          f"-> Failed to load or create Chroma collection '{collection_name}': {e}"
      )
      raise RuntimeError(f"ChromaDB initialization failed: {e}")

    if collection.count() == 0:
      self.logger.warning(
          f"-> Warning: Chroma collection '{collection_name}' is currently empty! "
          "Make sure you have ingested and embedded ASRS silver narratives into the vector store."
      )

    # 4. Correlate each telemetry anomaly with ASRS vector narratives
    diagnostic_results = []
    for row in anomaly_rows:
      row_dict = row.asDict()
      engine_id = row_dict.get("engine_id")
      cycle = row_dict.get("cycle")
      rul_val = row_dict.get("rul", "N/A")

      triggered_sensor = sensor_col[0]
      sensor_val = row_dict.get(triggered_sensor)
      sensor_val_float = (
          float(sensor_val) if sensor_val is not None else 0.0
      )

      query_text = (
          f"Engine {engine_id} anomaly at cycle {cycle} ({triggered_sensor}):"
          f" sensor value {sensor_val_float:.2f} and remaining useful life"
          f" {rul_val}."
      )
      self.logger.info(f"-> Querying vector store with: '{query_text}'")

      embed_response = ollama.embeddings(
          model="nomic-embed-text", prompt=query_text
      )
      query_embedding = embed_response["embedding"]

      results = collection.query(
          query_embeddings=[query_embedding], n_results=1
      )

      matched_doc = (
          results["documents"][0][0] if results["documents"] and results["documents"][0] else "No match found"
      )
      matched_metadata = (
          results["metadatas"][0][0] if results["metadatas"] and results["metadatas"][0] else {}
      )

      diagnostic_results.append({
          "engine_id": engine_id,
          "cycle": cycle,
          "sensor_egt": sensor_val,
          "query_text": query_text,
          "matched_asrs_narrative": matched_doc,
          "matched_metadata": str(matched_metadata),
      })

    # 5. Materialize diagnostic results as a Gold-layer Delta table
    diag_df = self.spark.createDataFrame(diagnostic_results)
    output_path = paths_cfg.get("gold_diagnostics", "data/lakehouse/gold/diagnostics")
    diag_df.write.format("delta").mode("overwrite").save(output_path)
    self.logger.info(
        f"-> Successfully wrote hybrid diagnostic results to {output_path}"
    )
    diag_df.show(truncate=False)