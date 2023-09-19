from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_column(
    dataframe: DataFrame, column_name: str, elements_to_preserve: list[str]
):
    """Filters a dataframe based on the `column_name` column's values preserving only those specified in the
    `elements_to_preserve` list

    :param dataframe: the dataframe we want to filter
    :type dataframe: DataFrame
    :param column_name: the name of the column from we want to filter by
    :type column_name: str
    :param elements_to_preserve: elements we want to preserve in the `column_name` column
    :type elements_to_preserve: list[str]
    :return: The dataframe which column specified in the input argument `column_name` has been filtered to contain
             only arguments specified in the `elements_to_preserve` list.
    :rtype: DataFrame
    """
    return dataframe.filter(col(column_name).isin(elements_to_preserve))


def rename_columns(dataframe: DataFrame, old_name_new_name_pairs: dict):
    """Renames dataframe columns.

    :param dataframe: dataframe which column names we want to change
    :type dataframe: DataFrame
    :param old_name_new_name_pairs: pairs defining the names of columns we want to change (keys)
                                    and their new names (values)
    :type old_name_new_name_pairs: dict
    :return: dataframe renamed according to the schema defined by `old_names_new_names_pairs`
    :rtype: DataFrame
    """
    for key, value in old_name_new_name_pairs.items():
        dataframe = dataframe.withColumnRenamed(key, value)
    return dataframe


def remove_columns(dataframe: DataFrame, columns_to_drop: list):
    """Removes columns specified in the `columns_to_drop` input list from the dataframe

    :param dataframe: dataframe from which we want to remove columns
    :type dataframe: DataFrame
    :param column_names_to_drop: names of the columns we want to remove
    :type column_names_to_drop: list
    :return: dataframe without specified columns
    :rtype: DataFrame
    """
    return dataframe.drop(*columns_to_drop)
