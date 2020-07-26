# Data Modelling with Apache Spark

## General

This project deals with a client Sparkify. They have grown their user base and song database and they want to migrate their data warehouse to a data late. Their data resides in AWS S3. The data is stored in JSON format.

The project contains an ETL pipeline that deals with extracting the data from S3 storing in a table and pushing it into S3. The data after being retrieved from S3 is stored in dimensional models in parquet format.

## Contents

The files found in this are the following :
 * dl.cfg: File that contains log in credentials for AWS. (They have been left out for security reasons.)
 * README.md: This is the current file. It contains information pertaining this object.
 * etl.py: This file contains the python module that performs the ETL process.
 
## Dimension Tables and Fact Table

songplays - Fact tableL: This table records log data associated with song plays. To create this table a Join was performed.

* songplay_id (INT) PRIMARY KEY: ID of each user song play
* start_time (DATE) NOT NULL: Timestamp of beggining of user activity
* user_id (INT) NOT NULL: ID of user
* level (TEXT): User level {free | paid}
* song_id (TEXT) NOT NULL: ID of Song played
* artist_id (TEXT) NOT NULL: ID of Artist of the song played
* session_id (INT): ID of the user Session
* location (TEXT): User location
* user_agent (TEXT): Agent used by user to access Sparkify platform

users - users in the app

* user_id (INT) PRIMARY KEY: ID of user
* first_name (TEXT) NOT NULL: Name of user
* last_name (TEXT) NOT NULL: Last Name of user
* gender (TEXT): Gender of user {M | F}
* level (TEXT): User level {free | paid}

songs - songs in music database

* song_id (TEXT) PRIMARY KEY: ID of Song
* title (TEXT) NOT NULL: Title of Song
* artist_id (TEXT) NOT NULL: ID of song Artist
* year (INT): Year of song release
* duration (FLOAT) NOT NULL: Song duration in milliseconds

artists - artists in music database

* artist_id (TEXT) PRIMARY KEY: ID of Artist
* name (TEXT) NOT NULL: Name of Artist
* location (TEXT): Name of Artist city
* lattitude (FLOAT): Lattitude location of artist
* longitude (FLOAT): Longitude location of artist

time - timestamps of records in songplays broken down into specific units

* start_time (DATE) PRIMARY KEY: Timestamp of row
* hour (INT): Hour associated to start_time
* day (INT): Day associated to start_time
* week (INT): Week of year associated to start_time
* month (INT): Month associated to start_time
* year (INT): Year associated to start_time
* weekday (TEXT): Name of week day associated to start_time
