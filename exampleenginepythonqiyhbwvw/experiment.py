
from typing import Dict
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from py4j.java_gateway import JavaObject
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from dataproc_sdk import DatioPysparkSession, DatioSchema
from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues
import pyspark.sql.functions as f

class DataprocExperiment:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(DataprocExperiment.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def run(self, **parameters: Dict) -> None:
        """
        Execute the code written by the user.

        Args:
            parameters: The config file parameters
        """
        """self.__logger.info("Ejecutando experiment")
        clients_df.show()
        print(clients_df.count())
        print(clients_df.collect())
        print(clients_df.head())
        print(clients_df.take(5))
        print(clients_df.first())
        print(clients_df.limit(5).collect())
        clients_df.printSchema()
        contracts_df.show()
        # contracts_df.printSchema()
        products_df.show()
        # products_df.printSchema()
        # Ejercicios de la sesión 5 de engine
        logic = BusinessLogic()
        customers_df = self.read_parquet("t_fdev_customers", parameters)
        phones_df = self.read_parquet("t_fdev_phoner", parameters)
        # customers_df.printSchema()
        # phones_df.printSchema()
        # phones_df.show()
        # customers_df.show()
        phones_filtered = logic.filter_by_date_phones(phones_df)
        customers_filtered = logic.filter_by_date_costumers(customers_df)
        # print(logic.join_tables_3(customers_df, phones_df).count())
        join_tables_3 = logic.join_tables_3(customers_filtered, phones_filtered)
        print("Regla 3:")
        join_tables_3.show()
        print(join_tables_3.count())
        print("Regla 4:")
        filter_vip_df = logic.filter_vip(join_tables_3)
        print(filter_vip_df.count())
        print("Regla 5:")
        discount_extra_df = logic.discount_extra(join_tables_3)
        print(discount_extra_df.count())
        print("Regla 6:")
        discount_extra_df_6 = logic.discount_extra_6(join_tables_3)
        final_price_6 = logic.final_price(discount_extra_df_6)
        final_price_6.show()
        print("Regla 7:")
        # discount_extra_df_7 = logic.discount_extra_6(join_tables_3)
        final_price_7 = logic.final_price_7(join_tables_3)
        top50_df = logic.count_top_50(final_price_7)
        print("Regla 8:")
        nfc_count = logic.replace_nfc(top50_df)
        nfc = logic.count_no_records(nfc_count)
        print(nfc)
        print("Regla 9:")
        date = self.get_date_config("jwk_date", parameters)
        print(date)
        date_df = logic.agg_jwk_date(join_tables_3, date)
        print("Regla 10:")
        age_df = logic.calculate_date(date_df)
        age_df.printSchema()
        age_df.show()
        age_df.write.mode("overwrite") \
            .partitionBy("jwk_date") \
            .option("partitionOverwriteMode", "dynamic") \
            .parquet(str(parameters["output_2"]))
        clients_df = self.read_csv("clients", parameters)
        contracts_df = self.read_csv("contracts", parameters)
        products_df = self.read_csv("products", parameters)
        filtered_clients_df = logic.filter_by_age_and_vip(clients_df)
        # filtered_clients_df.show()
        joined_df = logic.join_tables(clients_df, contracts_df, products_df)
        # joined_df.show()
        filtered_by_contracts_df = logic.filter_by_number_of_contracts(joined_df)
        # filtered_by_contracts_df.show()
        hashed_df = logic.hash_columns(filtered_by_contracts_df)
        # hashed_df.show(20, False)
        # self.__spark.read.parquet("resources/data/output/final_table/cod_producto=700/activo=false").show()
        hashed_df.write.mode("overwrite")\
            .partitionBy("cod_producto", "activo")\
            .option("partitionOverwriteMode", "dynamic")\
            .parquet(str(parameters["output"]))
        customers_df = self.read_parquet("t_fdev_customers", parameters)
        # customers_df.show()"""

        # Video 7 de engine
        # LECTURA
        init_values = InitValues()
        init_values.initialize_inputs(parameters)
        clients_df, contracts_df, products_df, output_path, output_schema = \
            init_values.initialize_inputs(parameters)
        # TRANSFORMACIONES
        logic = BusinessLogic()
        filtered_clients_df = logic.filter_by_age_and_vip(clients_df)
        joined_df = logic.join_tables(clients_df, contracts_df, products_df)
        filtered_by_contracts_df = logic.filter_by_number_of_contracts(joined_df)
        hashed_df = logic.hash_columns(filtered_by_contracts_df)
        final_df = hashed_df\
            .withColumn("hash", f.when(f.col("activo") == "false", f.lit("0"))
                        .otherwise(f.col("hash"))).filter(f.col("hash") == "0")
        final_df.printSchema()
        # ESCRITURA
        self.__datio_pyspark_session.write().mode("overwrite")\
            .option("partitionOverwriteMode", "dynamic")\
            .partition_by(["cod_producto", "activo"])\
            .datio_schema(output_schema)\
            .parquet(logic.select_all_columns(final_df), output_path)

    """def read_csv(self, table_id, parameters):
        return self.__spark.read\
            .option("header", "true")\
            .option("delimiter", ",")\
            .option("inferschema", "true")\
            .csv(str(parameters[table_id]))

    def read_parquet(self, table_id, parameters):
        return self.__spark.read.parquet(str(parameters[table_id]))

    def read_parquet_2(self, table_id, parameters):
        return self.__spark.read.option("header", "true").parquet(str(parameters[table_id]))"""

    def get_date_config(self, table_id, parameters):
        return str(parameters[table_id])
