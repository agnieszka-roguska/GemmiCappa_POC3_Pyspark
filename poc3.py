from pyspark.sql import SparkSession
from pyspark.sql.functions import col

import functions

working_directory = '/mnt/c/users/aroguska/PySpark_UpSkill_v0_1_1/poc3/data'

clients_path = working_directory + '/clients.csv'
financial_path = working_directory + '/financial.csv'

spark = (SparkSession.builder
         .appName('poc')
         .getOrCreate()
         )

#BRONZE DATA - raw data read from the file 
clients_DB = (spark
              .read
              .option('header', True)
              .option('delimiter', ',')
              .csv(clients_path)
              )

financial_DB = (spark
              .read
              .option('header', True)
              .option('delimiter', ',')
              .csv(financial_path)
              )

df = clients_DB.join(financial_DB, 'id')

column_names_to_change = ['cc_t', 'cc_n', 'cc_mc', 'a', 'ac_t']
column_names_new = ['credit_card_type', 'credit_card_number', 'credit_card_main_currency', 'active', 'account_type']

df = functions.filter_countries(df, ['Poland', 'France'])
df = functions.rename_columns(df, column_names_to_change, column_names_new)
df = functions.remove_personal_identifiable_informations(df)

df.show()