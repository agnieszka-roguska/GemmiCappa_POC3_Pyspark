from pyspark.sql import *
from pyspark.sql.functions import col


def filter_column(dataframe : DataFrame, column_name : str, elements_to_preserve : list[str]):
    return dataframe.filter(col(column_name).isin(elements_to_preserve))


def rename_columns(dataframe : DataFrame, old_name_new_name_pairs : dict):
    for key, value in old_name_new_name_pairs.items():
        dataframe = dataframe.withColumnRenamed(key, value)
    return dataframe


def remove_personal_identifiable_information(dataframe : DataFrame, column_names_to_drop):
    return dataframe.drop(*column_names_to_drop)