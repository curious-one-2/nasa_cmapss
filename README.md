# NASA C-MAPSS & ASRS Predictive Maintenance and RAG Pipeline (`nasa-pyspark-pipeline`)

## Purpose
This project implements a local PySpark data engineering and Retrieval-Augmented Generation (RAG) diagnostic pipeline for turbofan engine predictive maintenance. It processes NASA C-MAPSS telemetry (`train`, `test`, and `RUL` ground-truth splits) through Medallion Delta Lake layers to predict Remaining Useful Life (RUL), detect scale-calibrated sensor volatility and trend anomalies, and correlate flagged engine cycles with Aviation Safety Reporting System (ASRS) narratives for root-cause context.

# Description
Prognostics and health management is an important topic in industry for predicting state of assets to avoid downtime and failures. This data set is the Kaggle version of the very well known public data set for asset degradation modeling from NASA. It includes Run-to-Failure simulated data from turbo fan jet engines.

Engine degradation simulation was carried out using C-MAPSS. Four different were sets simulated under different combinations of operational conditions and fault modes. Records several sensor channels to characterize fault evolution. The data set was provided by the Prognostics CoE at NASA Ames.

# Prediction Goal
In this dataset the goal is to predict the remaining useful life (RUL) of each engine in the test dataset. RUL is equivalent of number of flights remained for the engine after the last datapoint in the test dataset.

The datasets can be found at https://data.nasa.gov/dataset/cmapss-jet-engine-simulated-data

## Technical Stack

* **Distributed Data Processing:** Apache Spark (PySpark, `Window` functions)
* **Lakehouse Storage:** Delta Lake (Medallion architecture across Bronze, Silver, and Gold layers, including `fact_engine_telemetry_test` and `gold_diagnostics`)
* **Workflow Orchestration:** Prefect
* **Vector Database & Embeddings:** ChromaDB (`PersistentClient`) and Ollama (`nomic-embed-text`)
* **Numerical & Scientific Computing:** NumPy and Python (Python 3.12 virtual environment)
* **Testing & Development Tooling:** `pytest`, Visual Studio Code


## Project Architecture & Medallion Data Flow

`nasa-pyspark-pipeline` runs locally in a Python 3.12 virtual environment and orchestrates batch PySpark transformations, Delta Lake tables, and local Retrieval-Augmented Generation (RAG) correlation through Prefect flows.

### Medallion Data Flow

1. **Bronze Layer (Raw Ingestion):**
   * Ingests raw NASA C-MAPSS turbofan splits 
   -`train` (100 engines cycles to the failure)
   -`test` (cut before the failure)
   -`RUL` (the truth value)
   
   * Ingests Aviation Safety Reporting System (ASRS) narrative datasets taken from actual reports from the trubo fan planes such as Boeing 7X7 series crews into Bronze Delta tables with upfront schema checks.
2. **Silver Layer (Feature Engineering, Alignment & Vector Indexing):**
   * Conforms and clean the data
   * For the test datast's RUL file, it adds the engine id as the file implicitly ordered by such but without actually having these values in the source file 
   * Computes rolling window features (`roll_std_5` and `trend_5`) for sensors `2, 3, 4, 11, 12, and 15`
   * For test dataset it predicts the RUL Value at each engine's final cycle and stores it into `fact_engine_telemetry_test`
   * Generates local vector embeddings for ASRS silver narratives using Ollama (`nomic-embed-text`) and stores them in a ChromaDB `PersistentClient` collection.
3. **Gold Layer (Hybrid Diagnostics & RAG Correlation):**
   * `hybrid_diagnostics.py` applies RUL proximity checks alongside per-sensor `roll_std_5` and `trend_5` thresholds to isolate anomalous cycles.
   * Formats flagged anomaly rows into diagnostic text prompts, embeds each prompt via Ollama (`nomic-embed-text`), retrieves matching ASRS narratives from ChromaDB, and writes the combined output to the `gold_diagnostics` Delta table.



## Repository Structure

```text
nasa-pyspark-pipeline/
├── config/
│   ├── app/  
│       └── dev.yaml                       # Lakehouse paths, sensor thresholds, Chroma/Ollama settings
├── data/
│   ├── archive/                            # Store processed source files
│   ├── checkpoints/                        # Store checkpoints
│   ├── landing/                            # NASA C-MAPSS (train, test, RUL) and ASRS source files
│   ├── lakehouse/
│   │   ├── bronze/                         # 
│   │        └── asrs                       # Raw validated ASRS Delta table
│   │        └── cmapss_sensors_train       # Raw validated training Delta table
│   │        └── cmapss_sensors_test        # Raw validated test Delta tabl
│   │        └── rul                        # Raw validated rul Delta table
│   │   ├── silver/                         # 
│   │        └── asrs                       # Conformed ASRS Delta table
│   │        └── cmapss_sensors             # Conformed training Delta table
│   │        └── cmapss_sensors_test        # Conformed test Delta table
│   │        └── rul                        # Conformed Rul Delta table
│   │   └── gold/
│   │       └── diagnostics/                # gold_diagnostics Delta output table
│   │       └── dim_engine/                 # gold_engine Delta output table
│   │       └── fact_engine_telemetry/      # fact_engine_telemetry Delta output table
│   │       └── fact_engine_telemetry_test/ # fact_engine_telemetry_test Delta output table
│   └── quarentine/                         # quarentine for bad records
│   └── vector_store/                       # Persistent ChromaDB vector store directory
├── orchestration/
│   └── pipeline_flow.py                    # orchestration script  
├── src/
│   └── nasa_platform/
│       ├── core                            # 
│           ├── job_base.py                 # Base job contract and fail-fast config validation
│           ├── logger.py                   # Centralized logger setup without duplicate handlers
│           ├── registry.py                 # Explicit job lookup and registration mapping
│           ├── spark_session.py            # Local Delta Lake SparkSession factory
│       ├── etl                             # 
│           ├── asrs_bronze.py              # ingest ASRS raw data into bronze layer
│           ├── asrs_silver.py              # conform ASRS data in the silver layer
│       ├── jobs                            # 
│           ├── evaluate_cmapss.py          # Computing RMSE and the PHM asymmetric scoring metric.
│           ├── gold_cmapss_job.py          # 
│           ├── gold_cmapss_test.py         #
│           ├── hybrid_diagnostics.py       # Per-sensor anomaly evaluation and Chroma/Ollama correlation
│           ├── ingest_archive_cmapss.py    # Archives processed source file
│           ├── ingest_cmapss.py            # Reads cmapss file from landing folder and stores into bronze
│           ├── ingest_rul.py               # Reads rul file from landing folder and stores into bronze
│           ├── silver_cmapss_job.py        # Conforms the cmapss train and test data in silver layer
│           ├── silver_rul_job.py           # Confirms the rul data in silver layer
│       ├── ml                              # 
│           ├── dataset_builder.py          # Dataset builder for C-MAPSS ML training and validation sets
│           ├── train_and_inference.py      # Train XGBoost Regressor and Run Batch Inference for C-MAPSS
│       ├── rag                             # 
│           ├── vector_ingest.py            # Reads ASRS and embeds in Ollama persists them into ChromaDB for vector RAG retrieval 
│       ├── transforms/ 
│           ├── silver/                     # 
│               └── cmapss_silver.py        # Transformation logic for cmapss silver layer 
│               └── rul_silver.py           # Transformation logic for rul silver layer  
│           ├── gold/                       #   
│               └── cmapss_gold.py          # Transformation logic for cmapss Gold layer             
├── tests/                                  # pytest unit and integration tests
│   ├── unit/                               # 
│        ├── conftest.py                    # 
│        ├── test_dataset_builder.py        #
│        ├── test_gold_cmapss.py            # 
│        ├── test_search.py                 #
│        ├── test_silver_cmapss.py          #      
├── README.md
└── requirements.txt
└── mlflow.db
└── pytest.ini 
```


## Key Results & Diagnostic Findings
* **Final-Cycle Alignment & RUL Accuracy:** the test data's RMSE=`3.7376`** cycles and achieving a PHM score of **`14.58`** across all **`100`** evaluated test units (engines).
* **Scale-Calibrated Sensor Thresholds:** Summary and percentile profiling of `roll_std_5` and `trend_5` across sensors `2, 3, 4, 11, 12, and 15` showed that **sensors 3 and 4** operate on a much wider dispersion and swing range than the remaining sensors, causing uniform global thresholds (`std > 1.5`, `trend > 2.0`) to over-trigger.  thresholds specific to each important sensors where customized.
* **Hybrid ASRS Vector Correlation:** Flagged telemetry rows are formatted into diagnostic prompts, embedded via Ollama (`nomic-embed-text`), queried against ASRS silver narratives in ChromaDB, and written out as a Gold-layer Delta table.