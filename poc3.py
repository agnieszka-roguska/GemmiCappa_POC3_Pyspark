import logging

def main(clients_path : str, financial_path : str, list_of_countries_to_preserve : list):

    import re

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    import functions
 
    FORMAT = '%(asctime)s:%(name)s:%(levelname)s - %(message)s'
    logging.basicConfig(format = FORMAT, level = logging.INFO)

    spark = (SparkSession.builder
                    .appName('poc')
                    .getOrCreate()
                    )

    #extracting working directory path from clients.csv file path
    working_directory = re.sub(r'/clients.csv', '', clients_path)

    #reading data from clients.csv and financial.csv files
    try:
        clients_DB = (spark
                    .read
                    .option('header', True)
                    .option('delimiter', ',')
                    .csv(clients_path)
                    )
        logging.info('Clients data was correctly extracted from the file.')
    except: 
        logging.critical("Unable to load data from clients.csv file")

    try:
        financial_DB = (spark
                    .read
                    .option('header', True)
                    .option('delimiter', ',')
                    .csv(financial_path)
                    )
        logging.info('Financial data was correctly extracted from the file.')
    except: 
        logging.critical("Unable to load data from financial.csv file")

    #creating new dataframe containing data from clients and financial files
    df = (clients_DB
          .join(financial_DB, 'id')
          .drop('id')
          )
    column_names_to_change = ['cc_t', 'cc_n', 'cc_mc', 'a', 'ac_t']
    column_names_new = ['credit_card_type', 'credit_card_number', 'credit_card_main_currency', 'active', 'account_type']

    #filtering out all clients from countries other than specified, removing the PPI and renaming columns as requested in a task
    df = functions.filter_countries(df, list_of_countries_to_preserve)
    logging.info('Successfully filtered out customers from countries other than: %s', list_of_countries_to_preserve)
    df = functions.remove_personal_identifiable_information(df)
    logging.info("Sucessfully removed all columns with personal identifiable information ")
    df = functions.rename_columns(df, column_names_to_change, column_names_new)
    logging.info("Successfully renamed abbreviated column names.")

    #writing data to the parquet file
    try:
        (df
        .write
        .mode('overwrite')
        .parquet(working_directory + '/client_data')
        )
        logging.info("Data was successfully written to a file.")
    except:
        logging.error("Unable to write data to a file.")

if __name__ == '__main__':

    working_directory = '/mnt/c/users/aroguska/PySpark_UpSkill_v0_1_1/poc3/data'
    list_of_countries_to_preserve = ['France', 'Poland']

    try:
        clients_path = working_directory + '/clients.csv'
        financial_path = working_directory + '/financial.csv'
        main(clients_path, financial_path, list_of_countries_to_preserve)
        logging.info("All done.")
    except:
        logging.critical("The app is not working.")