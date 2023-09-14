import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "--clients_path", type=str, required=True, help="path to clients dataset"
)
parser.add_argument(
    "--financial_path", type=str, required=True, help="path to financial dataset"
)
parser.add_argument(
    "--countries_to_preserve",
    type=str,
    required=True,
    help="list of countries we want to preserve; countries should be separated by commas",
)

args = parser.parse_args()

clients_path = args.clients_path
financial_path = args.financial_path
countries_to_preserve = args.countries_to_preserve.split(",")


def main(clients_path: str, financial_path: str, list_of_countries_to_preserve: list):

    import logging
    import re

    import functions
    from pyspark.sql import SparkSession

    FORMAT = "%(asctime)s:%(name)s:%(levelname)s - %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    spark = SparkSession.builder.appName("poc").getOrCreate()

    # extracting working directory path from clients.csv file path
    working_directory = re.sub(r"/clients.csv", "", clients_path)

    # reading data from clients.csv and financial.csv files
    try:
        clients_DB = (
            spark.read.option("header", True).option("delimiter", ",").csv(clients_path)
        )
        logging.info("Clients data was correctly extracted from the file.")
    except: logging.critical("Unable to load data from clients.csv file")

    try:
        financial_DB = (
            spark.read.option("header", True)
            .option("delimiter", ",")
            .csv(financial_path)
        )
        logging.info("Financial data was correctly extracted from the file.")
    except: logging.critical("Unable to load data from financial.csv file")

    # creating new dataframe containing data from clients and financial files
    df = clients_DB.join(financial_DB, "id").drop("id")

    column_names_to_change_old_new_pairs = {
        "cc_t": "credit_card_type",
        "cc_n": "credit_card_number",
        "cc_mc": "credit_card_main_currency",
        "a": "active",
        "ac_t": "account_type",
    }
    columns_with_PII = ["first_name", "last_name", "phone", "birthdate"]

    # filtering out all clients from countries other than specified, removing the PPI and renaming columns
    df = functions.filter_column(df, "country", list_of_countries_to_preserve)
    logging.info(
        "Successfully filtered out customers from countries other than: %s",
        list_of_countries_to_preserve,
    )
    df = functions.remove_personal_identifiable_information(df, columns_with_PII)
    logging.info(
        "Sucessfully removed all columns with personal identifiable information "
    )
    df = functions.rename_columns(df, column_names_to_change_old_new_pairs)
    logging.info("Successfully renamed abbreviated column names.")

    # writing data to the parquet file
    try:
        df.write.mode("overwrite").parquet(working_directory + "/client_data")
        logging.info("Data was successfully written to a file. All done!")
    except: logging.error("Unable to write data to a file.")


main(clients_path, financial_path, countries_to_preserve)
