def main(clients_path, financial_path, list_of_countries_to_preserve):

    import re
    import logging

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    import functions

    FORMAT = '%(asctime)s:%(name)s:%(levelname)s - %(message)s'
    logging.basicConfig(format = FORMAT, level = logging.INFO)

    logging.info('Info message from me to myself')

    working_directory = re.sub(r'/clients.csv', '', clients_path)

    spark = (SparkSession.builder
            .appName('poc')
            .getOrCreate()
            )

    #BRONZE DATA - raw data read from the file 
    try:
        clients_DB = (spark
                    .read
                    .option('header', True)
                    .option('delimiter', ',')
                    .csv(clients_path)
                    )
    except: 
        logging.critical("Couldn't load data from clients.csv file")

    try:
        financial_DB = (spark
                    .read
                    .option('header', True)
                    .option('delimiter', ',')
                    .csv(financial_path)
                    )
    except: 
        logging.critical("Couldn't load data fromfinancial.csv file")

    df = clients_DB.join(financial_DB, 'id')

    column_names_to_change = ['cc_t', 'cc_n', 'cc_mc', 'a', 'ac_t']
    column_names_new = ['credit_card_type', 'credit_card_number', 'credit_card_main_currency', 'active', 'account_type']

    df = functions.filter_countries(df, list_of_countries_to_preserve)
    df = functions.rename_columns(df, column_names_to_change, column_names_new)
    df = functions.remove_personal_identifiable_informations(df)

    #writing data to the parquet file
    (df
     .write
     .mode('overwrite')
     .parquet(working_directory + '/client_data')
    )

if __name__ == '__main__':

    working_directory = '/mnt/c/users/aroguska/PySpark_UpSkill_v0_1_1/poc3/data'
    clients_path = working_directory + '/clients.csv'
    financial_path = working_directory + '/financial.csv'
    list_of_countries_to_preserve = ['Poland', 'France']

    main(clients_path, financial_path, list_of_countries_to_preserve)