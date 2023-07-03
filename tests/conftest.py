"""Execute the pytest initialization.

Remove it under your own responsibility.
"""
import glob
import random
import sys
from pathlib import Path

from pyspark import Row
from pyspark.sql.types import StructType, StructField, StringType, DecimalType

import exampleenginepythonqiyhbwvw.common.constants as c
import pytest
from pyspark.sql import SparkSession

from exampleenginepythonqiyhbwvw.business_logic.business_logic import BusinessLogic
from exampleenginepythonqiyhbwvw.io.init_values import InitValues


@pytest.fixture(scope="session", autouse=True)
def spark_test():
    """Execute the module setup and initialize the SparkSession.

    Warning: if making any modifications, please make sure that this fixture is executed
    at the beginning of the test session, as the DatioPysparkSession will rely on it internally.

    Yields:
        SparkSession: obtained spark session

    Raises:
        FileNotFoundError: if the jar of Dataproc SDK is not found.
    """
    # Get Dataproc SDK jar path
    jars_path = str(Path(sys.prefix) / 'share' / 'sdk' / 'jars' / 'dataproc-sdk-all-*.jar')
    jars_list = glob.glob(jars_path)
    if jars_list:
        sdk_path = jars_list[0]
    else:
        raise FileNotFoundError(f"Dataproc SDK jar not found in {jars_path}, have you installed the requirements?")
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("unittest_job") \
        .config("spark.jars", sdk_path) \
        .master("local[*]") \
        .getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture(scope="session", autouse=True)
def business_logic():
    yield BusinessLogic()


@pytest.fixture(scope="session", autouse=True)
def init_values():
    yield InitValues()


@pytest.fixture(scope="session", autouse=True)
def clients_df(init_values):
    parameters_clients = {
        "clients_path": "resources/data/input/clients.csv",
        "clients_schema": "resources/schemas/clients_schema.json"
    }
    yield init_values.get_input_df(parameters_clients, c.CLIENTS_PATH, c.CLIENTS_SCHEMA)


@pytest.fixture(scope="session", autouse=True)
def contracts_df(init_values):
    parameters_contracts = {
        "contracts_path": "resources/data/input/clients.csv",
        "contracts_schema": "resources/schemas/clients_schema.json"
    }
    yield init_values.get_input_df(parameters_contracts, c.CONTRACTS_PATH, c.CONTRACTS_SCHEMA)


@pytest.fixture(scope="session", autouse=True)
def products_df(init_values):
    parameters_products = {
        "products_path": "resources/data/input/clients.csv",
        "products_schema": "resources/schemas/clients_schema.json"
    }
    yield init_values.get_input_df(parameters_products, c.PRODUCTS_PATH, c.PRODUCTS_SCHEMA)


@pytest.fixture(scope="session", autouse=True)
def clients_dummy_df(spark_test):
    data = [Row("111"), Row("123")]
    schema = StructType([
        StructField("cod_client", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)


@pytest.fixture(scope="session", autouse=True)
def clients_dummy_df_2(spark_test):
    data = [Row("111"), Row("123"),
            Row("111"), Row("333"),
            Row("111"), Row("444"),
            Row("111"), Row("666")]
    schema = StructType([
        StructField("cod_client", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)


@pytest.fixture(scope="session", autouse=True)
def contracts_dummy_df(spark_test):
    data = [Row("111", "aaa"), Row("123", "bbb")]
    schema = StructType([
        StructField("cod_titular", StringType()),
        StructField("cod_producto", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)


@pytest.fixture(scope="session", autouse=True)
def products_dummy_df(spark_test):
    data = [Row("aaa"), Row("bbb")]
    schema = StructType([
        StructField("cod_producto", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)


@pytest.fixture(scope="session", autouse=True)
def phones_dummy_df(spark_test):
    data = [Row("2020-03-01", "Acer", "MX", "aaa", "aaa"),
            Row("2020-02-01", "Dell", "IT", "bbb", "bbb"),
            Row("2020-03-03", "Acer", "BR", "ccc", "ccc"),
            Row("2020-03-03", "Acer", "BR", "ddd", "ddd"),
            Row("2020-02-01", "Dell", "IT", "eee", "eee")]
    schema = StructType([
        StructField("cutoff_date", StringType()),
        StructField("brand", StringType()),
        StructField("country_code", StringType()),
        StructField("customer_id", StringType()),
        StructField("delivery_id", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)


@pytest.fixture(scope="session", autouse=True)
def customers_dummy_df(spark_test):
    data = [Row("2020-03-01", "4432894348719044", "aaa", "aaa"),
            Row("2020-02-01", "4051221847882313550", "bbb", "bbb"),
            Row("2020-03-03", "375858512078559", "ccc", "ccc"),
            Row("2020-03-03", "36525268639055", "ddd", "ddd"),
            Row("2020-02-01", "4998113540430", "eee", "eee")]
    schema = StructType([
        StructField("gl_date", StringType()),
        StructField("credit_card_number", StringType()),
        StructField("customer_id", StringType()),
        StructField("delivery_id", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)


@pytest.fixture(scope="session", autouse=True)
def customers_phones_dummy_df(spark_test):
    data = [Row("Yes", "7500.00", "30", "Iphone"), Row("Yes", "6000.12", "30", "Iphone"),
            Row("No", "7500.01", "44", "Iphone"), Row("No", "8000.33", "30", "Iphone"),
            Row("Yes", "10000.44", "54", "Iphone"), Row("No", "3000.54", "30", "Panasonic"),
            Row("Yes", "1000.45", "30", "XOLO"), Row("No", "3456.44", "30", "Iphone")]
    schema = StructType([
        StructField("prime", StringType()),
        StructField("price_product", StringType()),
        StructField("stock_number", StringType()),
        StructField("brand", StringType())
    ])
    yield spark_test.createDataFrame(data, schema)
