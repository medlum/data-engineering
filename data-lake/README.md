

# Project

## The objective of this project is to build a ELT pipeline that extracts music streaming data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables for Sparkify analytics team to continue finding insights in what songs their users are listening to.


# ELT Pipeline

1. Set up an AWS EMR cluster to process data using Apache Spark
2. Create a Jupyter notebook on EMR to initiate the ELT process by:
    - Loading the song data files and log data files from S3 as Spark's dataframes.
    - Extracting the columns from data frames and transform into song table, artists table, users table, time table (dimension tables) and songplays table (fact table).



# ER Diagram of the Star Schema

An entity relationship diagram illustrates the design of a star schema aimed to improve the efficiency of SQL queries.

The schema is designed to reduce the need for multiple joins,improving the query speed among the analytics users.

![Star Schema](sparkify.png)


# Tables Creation



## Songplay Table (Fact Table)

Songplay table is the fact table with 9 columns. It is the last table to be created in execution, as it has to make references to the dimension table : 'time table'.

Otherwise, Spark will throw an error as the fact table makes reference to time table before the other dimension tables.

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
User table is a dimension table with 9 columns extracted as dataframe from log data files in S3.

|Column      |       DataType       |
|------------|----------------------|
|user_id     |  varchar primary key |
|first_name  |  varchar             |
|last_name   |  varchar             |
|gender      |  varchar             |
|level       |  varchar             |

## Song Table (Dimension Table)
Song table is a dimension table with 5 columns extracted as dataframe from song data files in S3.

|Column      |       DataType       |
|------------|----------------------|
|song_id     |  varchar primary key |
|title       |  varchar             |
|artist_id   |  varchar             | 
|year        |  int                 |
|duration    |  double precision    |

## Artist Table (Dimension Table)
Artist table is a dimension table with 5 columns extracted as dataframe from song data files in S3.

|Column      |       DataType       |
|------------|----------------------|
|artist_id   | varchar primary key  | 
|name        | varchar              |
|location    | varchar              |
|latitude    | double precision     |
|longitude   | double precision     |


## Time Table (Dimension Table)
Time table is a dimension table with 7 columns extracted as dataframe from log data files in S3.

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

Note that spark is executed via SSH to the EMR cluster, there is no need to setup aws access ID and secret access key.
Therefore, ETL implementation steps are as follows using a scripted python file `etl.py`:

1. Setup EMR cluster with 1 master and 5 core nodes (m5x large) on software config of emr 5.20.0.
2. Setup a 'sparkify-user' bucket in S3 to receive the partition files from the Spark job.
3. Copy etl.py to S3 from local and .then from S3 to EMR using AWS CLI
4. Use the spark-submit function to execute etl.py in AWS CLI
    - Extract the json files  from S3 buckets as Spark dataframe.
    - Load the columns from song data and log data to create the dimensions and fact tables.
    - Write the transformed data as parquet files back to S3 buckets for analytics purpose.
5. SHH to the cluster public DNS to use Spark UI to analyze the spark job.


