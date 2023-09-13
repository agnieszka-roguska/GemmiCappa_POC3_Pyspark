# GemmiCappa_POC3_Pyspark

### Table of contents
- [Introduction](#introduction)
- [Componets](#components)
- [Input parameters](#input-parameters)
- [Input datasets structure](#input-datasets-stuctures)
- [Result dataset structure](#resulted-dataset-stucture)

<a ID = "Introduction"></a>

### Introduction

In this project we load data from two datasets: dataset containing information about the clients and the dataset containing their financial data. We combine clients and financial information into one dataframe and filter clients by their nationality. The result is the parquet file with financial data of the clients from specified countries.

All abbreviated column names are replaced with full variable names. The output dataframe does not contain personal identifiable information. 

<a ID = "Components"></a>

### Components

The project is composed of three python files: 
- poc3.py : File containing the main function.
- functions.py : File with function definitions.
- tests.py : File with tests. 

<a ID = "Input parameters"></a>

### Input parameters

Application recieves 3 arguments:
- path to the `clients.csv` dataset
- path to the `financial.csv` dataset
- list of countries whose citizens we want to preserve in the dataframe

<a ID = "Input datasets column structure"></a>

### Input datasets column structure
#### clients.csv
|name|type|short decription|
|--|--|--|
|id|int|unique numeric identifier|
|first_name|str|client's first name|
|last_name|str|client's surname|
|email|str|client's email address|
|gender|str|client's gender|
|country|str|client's home country|
|phone|str|client's phone number| 
|birthdate|date|client's birth date|

#### financial.csv

|name|type|short decription|
|--|--|--|
|id|int|unique numeric identifier|
|cc_t|str|client's credit card type|
|cc_n|bigint|client's credit card number|
|cc_mc|str|client's credit card main currency|
|a|bool|bool determining if account is active|
|ac_t|str|client's account type|

<a ID = "Resulted dataset stucture"></a>
### Resulted dataset stucture

#### client_data.parquet

|name|type|short decription|
|--|--|--|
|email|str|client's email address|
|gender|str|client's gender|
|country|str|client's home country|
|credit_card_type|str|client's credit card type|
|credit_card_number|bigint|client's credit card number|
|credit_card_main_currency|str|client's credit card main currency|
|active|bool|bool determining if account is active|
|account_type|str|client's account type|

All abbreviated column names were renamed according to the table above:

|old name|new name|
|--|--|
|cc_t|credit_card_type|
|cc_n|credit_card_number|
|cc_mc|credit_card_main_currency|
|a|active|
|ac_t|account_type|
