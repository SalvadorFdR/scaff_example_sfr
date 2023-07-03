from unittest import TestCase
from unittest.mock import MagicMock

import pytest
from pyspark.sql import DataFrame
from dataproc_sdk import DatioSchema

from exampleenginepythonqiyhbwvw.config import get_params_from_runtime
from exampleenginepythonqiyhbwvw.io.init_values import InitValues
import exampleenginepythonqiyhbwvw.common.constants as c
import exampleenginepythonqiyhbwvw.common.input as i


class TestApp(TestCase):

    @pytest.fixture(autouse=True)
    def spark_session(self, spark_test):
        self.spark = spark_test

    def test_initialize_inputs(self):
        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")
        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "EnvironmentVarsPM"
        parameters = get_params_from_runtime(runtime_context, root_key)
        init_values = InitValues()
        clients_df, contracts_df, products_df, output_file, output_schema = init_values.initialize_inputs(parameters)
        self.assertEquals(type(clients_df), DataFrame)
        self.assertEquals(type(contracts_df), DataFrame)
        self.assertEquals(type(products_df), DataFrame)
        self.assertEquals(type(output_file), str)
        self.assertEquals(type(output_schema), DatioSchema)
        self.assertEquals(products_df.columns, [i.cod_producto.name, i.desc_producto.name])

    def test_get_input_df(self):
        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")
        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "EnvironmentVarsPM"
        parameters = get_params_from_runtime(runtime_context, root_key)
        init_values = InitValues()
        clients_df = init_values.get_input_df(parameters, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)
        contracts_df = init_values.get_input_df(parameters, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)
        products_df = init_values.get_input_df(parameters, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)
        self.assertEquals(type(clients_df), DataFrame)
        self.assertEquals(type(contracts_df), DataFrame)
        self.assertEquals(type(products_df), DataFrame)
        self.assertEquals(clients_df.columns, [i.cod_client.name,
                                               i.nombre.name,
                                               i.edad.name,
                                               i.provincia.name,
                                               i.cod_postal.name,
                                               i.vip.name])
        self.assertEquals(contracts_df.columns, [i.cod_iuc.name,
                                                 i.cod_titular.name,
                                                 i.cod_producto.name,
                                                 i.fec_alta.name,
                                                 i.activo.name])
        self.assertEquals(products_df.columns, [i.cod_producto.name,
                                                i.desc_producto.name])

    def test_config_by_name(self):
        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")
        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "EnvironmentVarsPM"
        parameters = get_params_from_runtime(runtime_context, root_key)
        init_values = InitValues()
        out_path, output_schema = init_values.get_config_by_name(parameters,
                                                                 c.OUTPUT_PATH_2,
                                                                 c.OUTPUT_SCHEMA_2)
        self.assertEquals(type(out_path), str)
        self.assertEquals(type(output_schema), DatioSchema)
