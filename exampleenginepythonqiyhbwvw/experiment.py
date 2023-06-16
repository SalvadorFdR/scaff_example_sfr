
from typing import Dict
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import SparkSession

class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__spark = SparkSession.builder.getOrCreate()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        self.__logger.info("Ejecutando experiment")
        clients_df = self.read_csv("clients",parameters)
        contracts_df = self.read_csv("contracts", parameters)
        products_df = self.read_csv("products", parameters)
        clients_df.show()
        """print(clients_df.count())
        print(clients_df.collect())
        print(clients_df.head())
        print(clients_df.take(5))
        print(clients_df.first())"""
        print(clients_df.limit(5).collect())
        clients_df.printSchema()
        contracts_df.show()
        # contracts_df.printSchema()
        products_df.show()
        # products_df.printSchema()

    def read_csv(self, table_id, parameters):
        return self.__spark.read\
            .option("header", "true")\
            .option("delimiter", ",")\
            .option("inferschema", "true").csv(str(parameters[table_id]))

