## How to run the ETL process.
1) Setup aws redshift cluster
2) Enter your aws redshift crendentials in dwh.cfg
3) Run the following commands

```
import create_tables as ct;
ct.main;
import etl as e;
e.main();
```

## Purpose of this database in the context of the startup, Sparkify, and their analytical goals.

 - This database contains analytical information about the usage of Sparkify users song play represented as a fact table using star schema. It also contains dimension data of user information, songs, artists and timestamps of records in songplay.
 - Sparkify oftens want to know where their user is located at, and what songs interest them. So that they can procure better song content with music provider.
 - Sparkify also wants to popular song and artist in the area so that they can do song recommendation to their user in similar location.

## Database schema design and ETL pipeline.
 - The data is modelled using star schema. Song play data is represented as a fact table and user information, songs, artists and timestamps of records in songplay is represented as dimension table.
 - The ETL pipeline extract JSON files from s3, then load them into staging tables in redshift. The next step is to perform SQL transformation and load them fact and dimension table into actual star schema tables in redshift.

## Directory

 - create_tables.py : Contains the logic to create tables based on sql_queries constants.
 - dwh.cfg : Notebook for testing sql queries.
 - etl.py: etl main file. You can run main() method here to start the etl process, but you need to run create_tables.main() first
 - README.md: This file that you are reading.
 - sql_queries.py: python file that contains create tables, insert  and select, unload SQL queries string.
