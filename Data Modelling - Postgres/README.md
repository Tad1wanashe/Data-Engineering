# Data Modelling with Postgres

This project is to create a Database for a client called Sparkify. They have their data in JSON logs that need to be extracted and populated into a Postgre database which has a star schema to help optimize query performance.

## Contents

* Jupyternotebook - Test. This notebook is to test if all the tables were loaded correctly.
* Sql_queries.py
* etl.py
* etl.ipynb
* create_tables.py
* Images
* Source files were included as examples of the data being manipulated.

## Tables 

* Song_play (Fact Table)
* Users (Dimension)
* songs (Dimension)
* Artists (Dimension)
* Time (Dimension)

## ETL Process

* Create_tables.py - Consists of the functions to create the database and above mentioned tables. Each time the file runs the first time it drops the database if it existed and the creates it again. The same process is then repeated for the tables.
* Etl.py - Consists of the functions that opens our JSON files into data pandas. The dataframe rows are then turned into a dictionary and then inserts artists and song records.
		 - Time stamp column in the process_log_file is converted to date time and then inserts the values into the relevant table and process the rest of the files.
* Etl.ipynb - This notebook processes the songs data, artist, users, time and song play. The notebook has step by step explanation of how it happened.

## General

A conceptual data model was created to test out the theories and later turned into the physical data model. The physical data model was done in the sql_queries.py file. The database that was used was of Postgressql.
The ETL pipeline was first tested out in the etl.ipynb and only one record was extracted at a time and inserted into tables to determine if the pipeline was working accordingly. I was able to make use of data frames to do the data analysis.







