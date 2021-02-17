

# Project

## The objective of this project is to build a ETL pipeline that extracts music streaming data from S3, stages them in Redshift and transforms data into a set of dimensional tables for Sparkify analytics team to continue finding insights in what songs their users are listening to.


# ETL Pipeline

1. Set up a Redshift cluster.
1. Setup two staging tables in S3 to copy `log data` and `songs data` from data sources.
1. Copy `log data` and `songs data` from source to the staging tables.
1. Create star schema with a fact table and four dimension tables based on the project requirements.
1. Setup a fact table and four dimensions tables based on star schema.
1. Create insert statements to copy data from the staging tables to Redshift.

# ER Diagram of the Star Schema

An entity relationship diagram illustrates the design of a star schema aimed to improve the efficiency of SQL queries.

The schema is designed to reduce the need for multiple joins,improving the query speed among the analytics users.

![Star Schema](sparkify.png)


# Tables Creation
Two staging tables are setup to copy data from data sources to staging area in S3:
- `Staging events table` to copy `log data` from s3://udacity-dend/log_data
- `Staging songs table` to copy `song data` from s3://udacity-dend/song_data
- Note that `log data` has a json path at s3://udacity-dend/log_json_path.json

## Staging events table
Staging events table consists of 16 columns and data types:


| Column            DataType  |Column              DataType|Column               DataType|
| ----------------------------|----------------------------|-----------------------------|
| artist             (varchar)| length   (double precision)| sessionId(double precision) |
| auth               (varchar)| level         (varchar)    | song          (varchar)     |
| firstName          (varchar)| location      (varchar)    | status        (int)         |
| gender             (varchar)| method        (varchar)    | ts            (bigint)      |
| itemInSession          (int)| page          (varchar)    | userAgent     (varchar)     |
| lastName           (varchar)| registration  decimal      | userId        (varchar)     |


## Staging songs table
Staging songs table consists of 10 columns and data types.


| Column                  DataType   |Column              DataType|
| -----------------------------------|----------------------------|
| artist_id                (varchar) | duration (double precision)|
| artist_latitude (double precision) | num_songs         (int)    | 
| artist_location          (varchar) | song_id        varchar)    | 
| artist_longitude (double precision)| title         (varchar)    | 
| artist_name              (varchar) | year              (int)    | 

## Songplay Table (Fact Table)
Songplay is the fact table with 9 columns with `distribution style: ALL`.  

Fact table is the last table to be created in execution, as it has to make references to the dimension tables. 

Otherwise, AWS will  throw an error `no relation to time_table` as the fact table makes reference to time table before the other dimension tables.

| Column       |           DataType              |
| -------------|---------------------------------|
| songplay_id  | bigint identity(0,1) primary key|
| start_time   | timestamp references time_table |
| user_id      | varchar references user_table   |
| level        | varchar                         |
| song_id      |varchar references song_table    |
| artist_id    |varchar references artist_table  |
| session_id   |double precision                 |
| location     |varchar                          |
| user_agent   |varchar                          |


## User table (Dimension Table)
User table is a dimension table with 9 columns with `distribution style: ALL`

|Column      |       DataType       |
|------------|----------------------|
|user_id     |  varchar primary key |
|first_name  |  varchar             |
|last_name   |  varchar             |
|gender      |  varchar             |
|level       |  varchar             |

## Song Table (Dimension Table)
Song table is a dimension table with 5 columns  with `distribution style: ALL`

|Column      |       DataType       |
|------------|----------------------|
|song_id     |  varchar primary key |
|title       |  varchar             |
|artist_id   |  varchar             | 
|year        |  int                 |
|duration    |  double precision    |

## Artist Table (Dimension Table)
Artist table is a dimension table with 5 columns with `distribution style: ALL`

|Column      |       DataType       |
|------------|----------------------|
|artist_id   | varchar primary key  | 
|name        | varchar              |
|location    | varchar              |
|latitude    | double precision     |
|longitude   | double precision     |


## Time Table (Dimension Table)
Time table is a dimension table with 7 columns with `distribution style: ALL`.

The hour, day, week, month, year and weekday columns are extracted by converting timestamp to datetime object. 

|Column      |       DataType       |
|------------|----------------------|
|start_time  |timestamp primary key |
|hour        |int                   |
|day         |int                   | 
|week        |int                   | 
|month       |int                   |
|year        |int                   |
|weekday     |varchar               |

# ETL Implementation

The execution of the data pipeline consists of 3 python files, `sql_queries.py`, `create_tables.py`, `etl_py`.

## File 1: sql_queries.py
This file consists of `CREATE` statements to create all tables, issue `copy` command to AWS to copy data from source to the staging tables, `INSERT` statements to copy data from staging tables to Redshift.

## File 2: create_tables.py
This file executes the `CREATE` statements to all tables and drop any existing tables which may have been created earlier in S3 (staging areas) and Redshift (data warehouse)

## File 3: etl.py
This file issues command to copy data from data source to the staging tables, before inserting the copied data into the 5 tables created in Redshift.

### Loading the staging table
`load_staging_tables(cur, conn)` executes  2`copy` command created in sql_queries to copy data from
`s3://udacity-dend/log_data` and `s3://udacity-dend/song_data`.

The `staging_events_copy` and `staging_songs_copy` functions in `sql_queries.py` required parameters from `dwh.cfg` which consists of:

- `Amazon Resource Name (ARN)` : 'arn:aws:iam::988680640315:role/redShiftRole'
- `Log data path` : 's3://udacity-dend/log_json_path.json'
- `Log data source` : 's3://udacity-dend/log_data'
- `Song data source` : 's3://udacity-dend/song_data/A/A'

refer to `dwh.cfg` for details.

### Inserting data into Redshift
Finally, `insert_tables(cur, conn)` inserts data from staging events  and staging songs tables into the follwing tables to complete the ETL pipeline for data warehouse on cloud:
 - User table
 - Song table
 - Artist table
 - Time table
 - Songplay table (fact table)


