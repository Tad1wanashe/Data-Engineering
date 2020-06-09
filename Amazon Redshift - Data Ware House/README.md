# Sparkify - Amazon Redshift Data Warehouse

This project is to create an Amazon Redshift Data Warehouse for a client called Sparkify. They have their data in S3 buckets that need to be transfered to staging tables and then from there to the relevant tables in relation to the schema selected. The schema that was chosen in this instance is a star schema.

## Tables
* staging_events
* staging_songs
* songplay (fact table)
* users (dimension)
* songs (dimension)
* artists (dimension)
* time (dimension)

## ETL Process
* Create_tables.py - Consists of the functions to create the above mentioned tables. Each time the file runs the first time it drops the table if they existed.
* etl.py - Consists of the functions that loads data in our staging tables and the insert into the tables created.
* sql_queries.py - The file loads the information we have from the dwh.cfg and comprises of the statements that drop and create the required tables. The tables are then populated with data. The staging tables populated using the copy function due to the large volumes of data to be moved over and all the other tables will be loaded from the staging tables.
* dwh.cfg - is the config file. Contains the database names/ cluster names and the credentials needed to get access to Amazon S3 buckets. You will need another Jupyter file to create your credentials or to log on to the Amazon site and create from there. For the purposes of this project the credentials were created using Infrastructure as a Code to give me more flexibility. The file is not attached for security reasons.

## General Information
Firstly, this project is completed by filing out all the required information in the sql_queries.py file. This file will do as mentioned above which is to Drop, create and load the tables. This file references the dwh.cfg which has the access information and cluster details to be able to create the data warehouse. After successful completion of the sql_queries.py file you execute the create_tables.py. This file will be in charge of running the sql_queries.py to create the needed tables. When this executes successfully you will then run the etl.py file which does the extracting and and loading of the data it makes use of the same file which is the sql_queries.py file.

The files include a file callled SongPlay Table that shows an example of what the table should look like when populated successfully.

## Technologies
* Amazon Redshift
* Python
* AWS - S3 buckets

## Contact
Feel free to contact me for any assistance with the project

