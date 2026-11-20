"""Vector Ingestion Job: Embeds ASRS silver narratives using Ollama and

persists them into ChromaDB for vector RAG retrieval.
"""

import logging
from pathlib import Path
import yaml
from pyspark.sql import functions as F
from nasa_platform.core.registry import register_job
from nasa_platform.core.job_base import JobBase


@register_job("ingest_asrs_vectors")
class VectorIngestJob(JobBase):

  def execute(self) -> None:
    self.logger.info(
        "-> Starting ASRS Vector Ingestion into ChromaDB via Ollama..."
    )

    env = self.config.get("env", "dev")
    config_path = Path(f"conf/app/{env}.yaml")
    if config_path.exists():
      with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    else:
      cfg = {
          "paths": {
              "silver_asrs": "data/lakehouse/silver/asrs",
              "vector_store": "data/vector_store",
          },
          "vector_store": {
              "collection_name": "nasa_asrs_incidents",
              "embedding_model": "nomic-embed-text",
          },
      }

    silver_path = cfg["paths"]["silver_asrs"]
    chroma_persist_dir = cfg["paths"]["vector_store"]
    collection_name = cfg["vector_store"]["collection_name"]
    embedding_model = cfg["vector_store"]["embedding_model"]

    # 1. Read Silver ASRS Delta table
    try:
      df = self.spark.read.format("delta").load(silver_path)
    except Exception as e:
      self.logger.error(f"-> Failed to read Silver ASRS table at {silver_path}: {e}")
      raise

    rows = df.collect()
    if not rows:
      self.logger.warning("-> No records found in Silver ASRS table.")
      return

    self.logger.info(
        f"-> Loaded {len(rows)} records from Silver ASRS. Generating embeddings..."
    )

    # 2. Initialize ChromaDB and Ollama
    import chromadb
    import ollama

    client = chromadb.PersistentClient(path=chroma_persist_dir)
    collection = client.get_or_create_collection(name=collection_name)

    # 3. Process each row safely, avoiding any missing schema keys
    for i, row in enumerate(rows):
      # Extract narrative text securely using available row fields
      row_dict = row.asDict()
      narrative = row_dict.get("narrative") or row_dict.get(
          "text", "No narrative available"
      )
      doc_id = str(
          row_dict.get("incident_id")
          or row_dict.get("id")
          or f"asrs_doc_{i}"
      )

      self.logger.info(f"-> Embedding document ID: {doc_id}")

      # Generate embedding via Ollama
      embed_response = ollama.embeddings(
          model=embedding_model, prompt=narrative
      )
      embedding = embed_response["embedding"]

      # Upsert into ChromaDB
      collection.upsert(
          ids=[doc_id],
          embeddings=[embedding],
          documents=[narrative],
          metadatas=[{
              "source": "ASRS Silver",
              "index": i,
          }],
      )

    self.logger.info(
        "-> Successfully ingested all ASRS vectors into ChromaDB collection:"
        f" '{collection_name}'"
    )