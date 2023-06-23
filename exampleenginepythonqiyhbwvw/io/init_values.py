from dataproc_sdk import DatioPysparkSession, DatioSchema
from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o
import exampleenginepythonqiyhbwvw.common.constants as c


class InitValues:

    def __init__(self):
        self.__logger = get_user_logger(InitValues.__qualname__)
        self.__datio_pyspark_session = DatioPysparkSession().get_or_create()

    def initialize_inputs(self, parameters):
        self.__logger.info("Using given configuration")
        clients_df = self.get_input_df(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)
        contracts_df = self.get_input_df(parameters, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)
        products_df = self.get_input_df(parameters, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)
        out_path, output_schema = self.get_config_by_name(parameters, c.OUTPUT_PATH, c.OUTPUT_SCHEMA)
        return clients_df, contracts_df, products_df, out_path, output_schema

    def get_input_df(self, parameters, key_path, key_schema):
        self.__logger.info("Reading from" + key_path)
        io_path, io_schema = self.get_config_by_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read().datioSchema(io_schema)\
            .option(c.HEADER, c.TRUE_STRING)\
            .option(c.DELIMITER, c.COMMA)\
            .csv(io_path)

    def initialize_inputs_2(self, parameters):
        self.__logger.info("Using given configuration")
        customers_df = self.get_input_df_2(parameters, c.T_FDEV_CUSTOMERS_PATH, c.T_FDEV_CUSTOMERS_SCHEMA)
        phones_df = self.get_input_df_2(parameters, c.T_FDEV_PHONES_PATH, c.T_FDEV_PHONES_SCHEMA)
        out_path, output_schema = self.get_config_by_name(parameters,
                                                          c.OUTPUT_PATH_2,
                                                          c.OUTPUT_SCHEMA_2)
        return customers_df, phones_df, out_path, output_schema

    def get_config_by_name(self, parameters, key_path, key_schema):
        self.__logger.info("Get config for" + key_path)
        io_path = parameters[key_path]
        io_schema = DatioSchema.getBuilder().fromURI(parameters[key_schema]).build()
        return io_path, io_schema

    def get_input_df_2(self, parameters, key_path, key_schema):
        self.__logger.info("Reading from" + key_path)
        io_path, io_schema = self.get_config_by_name(parameters, key_path, key_schema)
        return self.__datio_pyspark_session.read().datioSchema(io_schema)\
            .parquet(io_path)
