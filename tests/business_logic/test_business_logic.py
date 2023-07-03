from unittest import TestCase

from pyspark.sql import Window

import exampleenginepythonqiyhbwvw.common.input as i
import exampleenginepythonqiyhbwvw.common.output as o
import exampleenginepythonqiyhbwvw.common.constants as c
import pytest
import pyspark.sql.functions as f


class TestBusinessLogic(TestCase):

    @pytest.fixture(autouse=True)
    def spark_session(self,
                      clients_df,
                      contracts_df,
                      products_df,
                      clients_dummy_df,
                      clients_dummy_df_2,
                      contracts_dummy_df,
                      products_dummy_df,
                      phones_dummy_df,
                      customers_dummy_df,
                      customers_phones_dummy_df,
                      business_logic):
        self.clients_df = clients_df
        self.contracts_df = contracts_df
        self.products_df = products_df
        self.clients_dummy_df = clients_dummy_df
        self.clients_dummy_df_2 = clients_dummy_df_2
        self.contracts_dummy_df = contracts_dummy_df
        self.products_dummy_df = products_dummy_df
        self.phones_dummy_df = phones_dummy_df
        self.customers_dummy_df = customers_dummy_df
        self.customers_phones_dummy_df = customers_phones_dummy_df
        self.business_logic = business_logic

    def test_filter_by_age_and_vip(self):
        self.clients_filtered_df = self.business_logic.filter_by_age_and_vip(self.clients_df)
        self.assertEqual(self.clients_filtered_df.filter(i.edad() < c.THIRTY_NUMBER).count(), 0)
        self.assertEqual(self.clients_filtered_df.filter(i.edad() > c.FIFTY_NUMBER).count(), 0)
        self.assertEqual(self.clients_filtered_df.filter(i.edad() != c.TRUE_STRING).count(), 0)

    def test_join_tables(self):
        join_df = self.business_logic.join_tables(self.clients_dummy_df,
                                                  self.contracts_dummy_df,
                                                  self.products_dummy_df)
        total_expected_columns = len(self.clients_dummy_df.columns) + \
                                 len(self.contracts_dummy_df.columns) + \
                                 len(self.products_dummy_df.columns) - 1
        self.assertEqual(len(join_df.columns), total_expected_columns)

    def test_hash_column(self):
        output_df = self.business_logic.hash_columns(self.contracts_dummy_df)
        self.assertEqual(len(output_df.columns), len(self.contracts_dummy_df.columns) + 1)
        self.assertIn("hash", output_df.columns)

    def test_filter_by_number_of_contracts(self):
        output_df = self.business_logic.filter_by_number_of_contracts(self.clients_dummy_df_2)
        validation_df = output_df.select(*output_df.columns,
                                         f.count("cod_client").over(Window.partitionBy("cod_client"))
                                         .alias("count")).filter(f.col("count") <= 3).drop("count")
        self.assertEqual(validation_df.count(), 0)
        self.assertEqual(output_df.count(), 4)

    def test_filter_by_date_phones(self):
        output_df = self.business_logic.filter_by_date_phones(self.phones_dummy_df)
        validation_df = output_df.filter((i.cutoff_date() <= "2020-03-01") &
                                         (i.cutoff_date() >= "2020-03-04")
                                         & (i.brand() == "Dell")
                                         & (i.brand() == "Coolpad")
                                         & (i.brand() == "Chea")
                                         & (i.brand() == "BQ")
                                         & (i.brand() == "BLU")
                                         & (i.country_code() == "CH")
                                         & (i.country_code() == "IT")
                                         & (i.country_code() == "CZ")
                                         & (i.country_code() == "DK"))
        self.assertEqual(validation_df.count(), 0)
        self.assertEqual(output_df.count(), 3)

    def test_filter_by_date_costumers(self):
        output_df = self.business_logic.filter_by_date_costumers(self.customers_dummy_df)
        validation_df = output_df.filter((i.gl_date() <= "2020-03-01")
                                         & (i.gl_date() >= "2020-03-04")
                                         & (i.credit_card_number() > 1e17))
        self.assertEqual(validation_df.count(), 0)
        self.assertEqual(output_df.count(), 3)

    def test_join_tables_3(self):
        join_df = self.business_logic.join_tables_3(self.customers_dummy_df,
                                                    self.phones_dummy_df)
        total_expected_columns = len(self.customers_dummy_df.columns) + \
                                 len(self.phones_dummy_df.columns) - 2
        self.assertEqual(len(join_df.columns), total_expected_columns)

    def test_filter_vip(self):
        output_df = self.business_logic.filter_vip(self.customers_phones_dummy_df)
        validation_df = output_df.filter(o.customer_vip() == c.YES_STRING)
        self.assertEqual(validation_df.count(), 2)
        self.assertEqual(output_df.count(), 8)

    def test_discount_extra(self):
        output_df = self.business_logic.discount_extra(self.customers_phones_dummy_df)
        validation_df = output_df.filter(o.extra_discount() > 0.00)
        self.assertEqual(validation_df.count(), 2)
        self.assertEqual(output_df.count(), 8)
