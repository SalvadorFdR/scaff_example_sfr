from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from dataproc_sdk import DatioSchema

from exampleenginepythonqiyhbwvw.config import get_params_from_runtime
from exampleenginepythonqiyhbwvw.io.init_values import InitValues
from exampleenginepythonqiyhbwvw.common.input import cod_producto, desc_producto


def test_initialize_inputs(spark_test):
    config_loader = spark_test._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
    config = config_loader.fromPath("resources/application.conf")
    runtime_context = MagicMock()
    runtime_context.getConfig.return_value = config
    root_key = "EnvironmentVarsPM"
    parameters = get_params_from_runtime(runtime_context, root_key)
    init_values = InitValues()
    clients_df, contracts_df, products_df, output_file, output_schema = init_values.initialize_inputs(parameters)
    assert type(clients_df) == DataFrame
    assert type(contracts_df) == DataFrame
    assert type(products_df) == DataFrame
    assert type(output_file) == str
    assert type(output_schema) == DatioSchema
    assert products_df.columns == [cod_producto.name, desc_producto.name]

