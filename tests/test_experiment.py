from unittest import TestCase
from unittest.mock import patch, mock_open, MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from exampleenginepythonqiyhbwvw.experiment import DataprocExperiment
from exampleenginepythonqiyhbwvw.config import (
    get_params_from_job_env,
    get_params_from_config,
    get_params_from_runtime,
)


class TestApp(TestCase):

    @pytest.fixture(autouse=True)
    def spark_session(self, spark_test):
        self.spark = spark_test

    def test_run_experiment(self):
        config_loader = self.spark._jvm.com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader()
        config = config_loader.fromPath("resources/application.conf")
        runtime_context = MagicMock()
        runtime_context.getConfig.return_value = config
        root_key = "EnvironmentVarsPM"
        parameters = get_params_from_runtime(runtime_context, root_key)
        experiment = DataprocExperiment()
        experiment.run(**parameters)
        # Initialize Spark session
        spark = SparkSession.builder.appName("unittest_job").master("local[*]").getOrCreate()

        out_df = spark.read.parquet(parameters["output_path"])

        self.assertIsInstance(out_df, DataFrame)
