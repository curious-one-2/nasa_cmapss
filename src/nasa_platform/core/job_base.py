import logging
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class JobBase(ABC):
    """Abstract base class for all PySpark ETL and ML jobs."""
    
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Ensure logging is configured if not already set up globally
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            )

    @abstractmethod
    def execute(self) -> None:
        """Core execution logic to be implemented by each job subclass."""
        pass

    def run(self) -> None:
        """Lifecycle method wrapping execution with logging and metrics."""
        self.logger.info(f"Starting job: {self.__class__.__name__}")
        try:
            result= self.execute()
            self.logger.info(f"Successfully completed job: {self.__class__.__name__}")
            return result
        except Exception as e:
            self.logger.error(f"Job failed: {self.__class__.__name__} with error: {str(e)}")
            raise