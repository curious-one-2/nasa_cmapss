import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class LocalSparkFactory:
    @staticmethod
    def get_session(app_name: str = "NASAPipeline", master: str = "local[4]") ->SparkSession:
        return (SparkSession.builder
                .master(master)
                .appName(app_name)
                .config("spark.driver.memory", "4g")
                .config("spark.sql.shuffle.partitions", 12)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.extraJavaOptions", "-Dlog4j.configuration=file:config/log4j.properties")
                # Inject Delta Lake package & extensions for Spark 4.2 / Delta 4.4
                .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.4.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()
            )
