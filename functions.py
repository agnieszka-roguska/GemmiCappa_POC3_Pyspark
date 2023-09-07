from pyspark.sql.functions import col

def filter_countries(dataframe, countries_to_preserve):
    return dataframe.filter(col('country').isin(countries_to_preserve))

def rename_columns(dataframe, column_names_to_change, column_names_new):
    mapping = dict(zip(column_names_to_change, column_names_new))
    return dataframe.select([col(x).alias(mapping.get(x, x)) for x in dataframe.columns])

def remove_personal_identifiable_informations(dataframe):
    return dataframe.drop('first_name', 'last_name', 'phone', 'birthdate', 'id')