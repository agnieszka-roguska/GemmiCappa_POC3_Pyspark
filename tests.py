import unittest

import chispa
from pyspark.sql import SparkSession
import functions


class TestMethods(unittest.TestCase):
    spark = SparkSession.builder.master("local").appName("chispa").getOrCreate()

    sample_data = [
        (
            "Natalie",
            "Wetta",
            "abc@gmail.com",
            "male",
            "Turkey",
            "603-679-1010",
            "1333-04-05",
            "376543576978",
            "EUR",
            True,
            "acc_type1",
        ),
        (
            "Thomy",
            "Kessler",
            "def@gmail.com",
            "female",
            "United States",
            "603-679-5671",
            "1333-04-05",
            "312563478",
            "EUR",
            True,
            "acc_type3",
        ),
        (
            "Melissa",
            "McQueen",
            "ghi@gmail.com",
            "male",
            "Spain",
            "603-679-1978",
            "1333-04-05",
            "3364069796",
            "EUR",
            False,
            "acc_type9",
        ),
        (
            "Rich",
            "Sommer",
            "jkl@gmail.com",
            "male",
            "Poland",
            "603-679-3186",
            "1333-04-05",
            "9764131867",
            "UAH",
            True,
            "acc_type9",
        ),
        (
            "Finn",
            "Wittrock",
            "mno@gmail.com",
            "female",
            "Russia",
            "548-787-4519",
            "1333-04-05",
            "273979677",
            "RUB",
            False,
            "acc_type1",
        ),
        (
            "Jordanna",
            "Oberman",
            "prs@gmail.com",
            "female",
            "Iran",
            "603-679-1426",
            "1333-04-05",
            "364217694",
            "EUR",
            False,
            "acc_type2",
        ),
        (
            "Emily",
            "Goss",
            "tuw@gmail.com",
            "female",
            "Hungary",
            "414-240-5284",
            "1333-04-05",
            "373896648",
            "CNY",
            False,
            "acc_type2",
        ),
        (
            "Joshua",
            "Bevier",
            "xyz@gmail.com",
            "male",
            "United States",
            "603-679-1685",
            "1333-04-05",
            "987546195",
            "EUR",
            True,
            "acc_type3",
        ),
        (
            "Gwendolyn",
            "Edwards",
            "123@gmail.com",
            "female",
            "Czech Republic",
            "603-679-2947",
            "1333-04-05",
            "97765433",
            "MDL",
            True,
            "acc_type1",
        ),
    ]

    sample_columns = [
        "first_name",
        "last_name",
        "email",
        "gender",
        "country",
        "phone",
        "birthdate",
        "credit_card_number",
        "credit_card_main_currency",
        "active",
        "account_type",
    ]
    sample_dataframe = spark.createDataFrame(sample_data, sample_columns)

    def test_filter_column_function_filters_out_countries_so_that_only_defined_ones_left(
        self,
    ):
        countries_to_preserve = [("Czech Republic"), ("Spain"), ("United States")]
        expected = self.spark.createDataFrame(countries_to_preserve, "string").toDF(
            "country"
        )
        result = functions.filter_column(
            self.sample_dataframe, "country", countries_to_preserve
        )
        result = result.select("country").distinct()
        chispa.assert_df_equality(expected, result, ignore_row_order=True)

    def test_rename_columns_takes_dataframe_as_parameter_and_returnes_dataframe_with_certain_column_renamed(
        self,
    ):
        column_names_old_new_pairs = {
            "email": "e-mail",
            "gender": "sex",
            "credit_card_main_currency": "main_currency",
        }
        expected = self.spark.createDataFrame(
            self.sample_data,
            [
                "first_name",
                "last_name",
                "e-mail",
                "sex",
                "country",
                "phone",
                "birthdate",
                "credit_card_number",
                "main_currency",
                "active",
                "account_type",
            ],
        )
        result = functions.rename_columns(
            self.sample_dataframe, column_names_old_new_pairs
        )
        chispa.assert_df_equality(expected, result)

    def test_remove_personal_identifiable_informations_returns_given_dataframe_without_columns_containing_PII(
        self,
    ):
        expected_column_names = [
            element
            for element in self.sample_columns
            if element not in ["first_name", "last_name", "phone", "birthdate"]
        ]
        expected = self.sample_dataframe.select(expected_column_names)
        result = functions.remove_personal_identifiable_information(
            self.sample_dataframe, ["first_name","last_name", "phone", "birthdate"]
        )
        chispa.assert_df_equality(expected, result)


unittest.main()
