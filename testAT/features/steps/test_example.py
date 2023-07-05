import sys
from behave import Given, Then, When
from behave.runner import Context
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from testAT.features.environment import init_spark_session
from worker import main


@Given(u'a config file {config_file}')
def set_config(context: Context, config_file):
    conf_path = "%s/%s" % (context.resources_dir, config_file)
    context.config_file = conf_path


@When(u'execute example app file in PySpark')
def execute_app(context: Context):
    sys.argv = ["local", context.config_file]
    context.return_code = main()


@Then(u'result should be {exit_code}')
def check_exit(context: Context, exit_code):
    assert context.return_code == int(exit_code)


@Given(u'an output dataFrame located at path {output_path}')
def set_output_path(context: Context, output_path):
    context.output_path = output_path


@When(u'read the output dataFrame')
def execute_output_path(context: Context):
    spark = SparkSession.builder.getOrCreate()
    spark.stop()
    spark = init_spark_session()
    context.output_df = spark.read.parquet(context.output_path)


@Then(u'total columns should be equal to {tot_columns}')
def check_total_columns(context: Context, tot_columns):
    assert len(context.output_df.columns) == int(tot_columns)


@Then(u'{column_name} column should {comparator} {value}')
def check_columns_values(context: Context, column_name, comparator, value):
    if comparator == "be gr or eq":
        assert context.output_df.filter(col(column_name) < float(value)).count() == 0
    if comparator == "be lr or eq":
        assert context.output_df.filter(col(column_name) > float(value)).count() == 0
    if comparator == "be eq":
        assert context.output_df.filter(col(column_name) != value).count() == 0
    if comparator == "be like":
        assert context.output_df.filter(~col(column_name).rlike(value)).count() == 0

# Para la validación de phones-customers


@Given(u'an output dataFrame phones_customers located at path {output_path}')
def set_output_path_customers_phones(context: Context, output_path):
    context.output_path = output_path


@When(u'read the output dataFrame phones_customers')
def execute_output_path_customers_phones(context: Context):
    spark = SparkSession.builder.getOrCreate()
    spark.stop()
    spark = init_spark_session()
    context.output_df = spark.read.parquet(context.output_path)


@Then(u'total columns phones_customers should be equal to {tot_columns}')
def check_total_columns_customers_phones(context: Context, tot_columns):
    assert len(context.output_df.columns) == int(tot_columns)


@Then(u'{column_name} column phones_customers should {comparator} {value}')
def check_columns_values(context: Context, column_name, comparator, value):
    if comparator == "be eq":
        assert context.output_df.filter(col(column_name) == value).count() == 0
    if comparator == "be like":
        assert context.output_df.filter(~col(column_name).rlike(value)).count() == 0
    if comparator == "be eq 2":
        assert context.output_df.filter(col(column_name) != value).count() == 0
