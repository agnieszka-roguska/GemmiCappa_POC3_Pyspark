from pyspark.sql import SparkSession
from pyspark.sql.functions import col

clients_path = '/mnt/c/users/aroguska/PySpark_UpSkill_v0_1_1/poc3/data/clients.csv'
financial_path = '/mnt/c/users/aroguska/PySpark_UpSkill_v0_1_1/poc3/data/financial.csv'

spark = (SparkSession.builder
         .appName('poc')
         .getOrCreate()
         )
#
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


column_pair_old_new = {'cc_t' : 'credit_card_type',
                       'cc_n' : 'credit_card_number',
                       'cc_mc' : 'credit_card_main_currency',
                       'a' : 'active',
                       'ac_t' : 'account_type'
                       }

mapping = dict(zip(['cc_t', 'cc_n', 'cc_mc', 'a', 'ac_t'], ['credit_card_type', 'credit_card_number', 'credit_card_main_currency', 'active', 'account_type']))
"""for key, value in column_pair_old_new.items():
    df = df.withColumnRenamed(key, value)"""

df = (df
      .filter(col('country').isin(['France', 'Poland']))
      .select([col(x).alias(mapping.get(x, x)) for x in df.columns])
      .drop('first_name', 'last_name', 'phone', 'birthdate', 'id')
      )