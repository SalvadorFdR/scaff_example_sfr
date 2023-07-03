import configparser

from dataproc_sdk.dataproc_sdk_utils.logging import get_user_logger
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as f
import pyspark.sql.types as t
import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o


class BusinessLogic:
    """
    Just a wrapper class to ease the user code execution.
    """

    def __init__(self):
        """
        Constructor
        """
        self.__logger = get_user_logger(BusinessLogic.__qualname__)

    def select_all_columns(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Selecting all columns")
        return df.select(
            i.cod_producto().cast(t.StringType()),
            i.cod_iuc().cast(t.StringType()),
            i.cod_titular().cast(t.StringType()),
            i.fec_alta().cast(t.DateType()),
            i.activo().cast(t.BooleanType()),
            i.cod_client().cast(t.StringType()),
            i.nombre().cast(t.StringType()),
            i.edad().cast(t.IntegerType()),
            i.provincia().cast(t.StringType()),
            i.cod_postal().cast(t.IntegerType()),
            i.vip().cast(t.BooleanType()),
            i.desc_producto().cast(t.StringType()),
            o.hash().cast(t.StringType())
        )

    def select_all_columns_2(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Selecting all columns")
        return df.select(
            f.col("city_name").cast(t.StringType()),
            f.col("street_name").cast(t.StringType()),
            f.col("credit_card_number").cast(t.StringType()),
            f.col("last_name").cast(t.StringType()),
            f.col("first_name").cast(t.StringType()),
            f.col("age").cast(t.IntegerType()),
            f.col("brand").cast(t.StringType()),
            f.col("model").cast(t.StringType()),
            f.col("nfc").cast(t.StringType()),
            f.col("country_code").cast(t.StringType()),
            f.col("prime").cast(t.StringType()),
            f.col("customer_vip").cast(t.StringType()),
            f.col("taxes").cast(t.DecimalType(9, 2)),
            f.col("price_product").cast(t.DecimalType(9, 2)),
            f.col("discount_amount").cast(t.DecimalType(9, 2)),
            f.col("extra_discount").cast(t.DecimalType(9, 2)).alias("extra_discount"),
            f.col("final_price").cast(t.DecimalType(9, 2)),
            f.col("top_50").cast(t.IntegerType()).alias("brands_top"),
            f.col("jwk_date").cast(t.DateType())
        )

    def filter_by_age_and_vip(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter by ages and vip status")
        return df.filter((f.col("edad") >= 30) & (f.col("edad") <= 50) & (f.col("vip") == "true"))

    def filter_by_date_phones(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter by dates and excluding")
        return df.filter((f.col("cutoff_date") >= "2020-03-01") & (f.col("cutoff_date") <= "2020-03-04")
                         & (f.col("brand") != "Dell") & (f.col("brand") != "Coolpad") & (f.col("brand") != "Chea")
                         & (f.col("brand") != "BQ") & (f.col("brand") != "BLU") & (f.col("country_code") != "CH")
                         & (f.col("country_code") != "IT") & (f.col("country_code") != "CZ")
                         & (f.col("country_code") != "DK"))

    def filter_by_date_costumers(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter by dates and excluding")
        return df.filter((f.col("gl_date") >= "2020-03-01") & (f.col("gl_date") <= "2020-03-04")
                         & (f.col("credit_card_number") < 1e17))

    def filter_vip(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter vips:")
        df = df.withColumn("customer_vip",
                           f.when((f.col("prime") == "Yes") & (f.col("price_product") >= 7500.00), "Yes")
                           .otherwise("No"))
        return df

    def discount_extra(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter discount_extra:")
        df = df.withColumn("extra_discount",
                           f.when((f.col("prime") == "Yes") &
                                  (f.col("stock_number") < 35) &
                                  ((f.col("brand") != "XOLO") &
                                   (f.col("brand") != "Siemens") &
                                   (f.col("brand") != "Panasonic") &
                                   (f.col("brand") != "BlackBerry")),
                                  f.col("price_product") * 0.10)
                           .otherwise(0.00))
        # df = df.filter(f.col("discount_extra") > 0.00)
        return df

    def discount_extra_6(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter discount_extra:")
        df = df.withColumn("extra_discount",
                           f.when((f.col("prime") == "Yes") &
                                  (f.col("stock_number") < 35) &
                                  ((f.col("brand") != "XOLO") &
                                   (f.col("brand") != "Siemens") &
                                   (f.col("brand") != "Panasonic") &
                                   (f.col("brand") != "BlackBerry")),
                                  f.col("price_product") * 0.10)
                           .otherwise(0.00))
        # df = df.filter(f.col("discount_extra") > 0.00)
        return df

    def count_top_50(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Count top 50:")
        window = Window.partitionBy("brand").orderBy(f.col("final_price").desc())
        df = df.withColumn("rank", f.dense_rank().over(window))
        df = df.withColumn("top_50", f.when(f.col("rank") <= 50, "Entre al top 50").otherwise("No entre al top 50"))
        df = df.filter(f.col("top_50") == "Entre al top 50")
        print(df.count())
        df = df.drop("rank")
        return df

    def replace_nfc(self, df: DataFrame) -> DataFrame:
        df_modified = df.withColumn("nfc", f.when(f.col("nfc").isNull(), "No").otherwise(f.col("nfc")))
        return df_modified

    def count_no_records(self, df: DataFrame) -> int:
        no_count = df.filter(f.col("nfc") == "No").count()
        return no_count

    def agg_jwk_date(self, df: DataFrame, fecha: str) -> DataFrame:
        return df.withColumn("jwk_date", f.lit(fecha))

    def final_price(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter final_price:")
        df = df.withColumn("final_price", (f.col("price_product") + f.col("taxes") -
                                           f.col("discount_amount") - f.col("extra_discount")))
        return df

    def calculate_date(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying calculate age:")
        return df.withColumn("age", (f.floor((f.months_between(f.current_date(), f.col("birth_date")) / 12))))

    def final_price_7(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filter final_price:")
        df = df.withColumn("final_price", (f.col("price_product") + f.col("taxes") -
                                           f.col("discount_amount")))
        # df = df.select(f.avg(f.col("final_price")).alias("average_final"))
        return df

    def filter_customers_delivery(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying join process, phones, customers and filter")
        return df.filter((f.col("customer_id").isNotNull()) or (f.col("delivery_id").isNotNull()))

    def join_tables(self, clients_df: DataFrame, contracts_df: DataFrame, products: DataFrame) -> DataFrame:
        self.__logger.info("Applying join process")
        return clients_df.join(contracts_df, (f.col("cod_client") == f.col("cod_titular")), "inner") \
            .join(products, ["cod_producto"], "inner")

    def join_tables_3(self, customers_df: DataFrame, phones_df: DataFrame) -> DataFrame:
        self.__logger.info("Applying join process, phones, customers")
        joined_df = customers_df.join(phones_df, (["customer_id", "delivery_id"]), "inner")
        return joined_df

    def filter_by_number_of_contracts(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Applying filtering by number of contracts")
        return df.select(*df.columns, f.count("cod_client").over(Window.partitionBy("cod_client"))
                         .alias("count")).filter(f.col("count") > 3).drop("count")

    def hash_columns(self, df: DataFrame) -> DataFrame:
        self.__logger.info("Generating hash columns")
        return df.select(*df.columns, f.sha2(f.concat_ws("||", *df.columns), 256).alias("hash"))
