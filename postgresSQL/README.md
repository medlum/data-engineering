# Instructions on how to run the scripts
1. Execute create.py on console to create SQL tables
2. Execute etl.py on console to extract and load data into SQL tables
3. Use test.ipynb to test query to check if data are loaded

# Briefly explain the files in the repository
The log file and song file are stored in separate data folder. Log files consists of the users details
when streaming music with Sparkify platform while song file contains the details each song.

# State and justify your database schema design and ETL pipeline. 

The schema design consists of the following dimension tables based on the log data and song data:

The log data are divided into 3 dimension tables:

1. Time Table : where timestamp are extracted into : hour, day, week_day, month, year  
2. User Table : userid, firstname, lastname, gender, level
3. Songplay Table : timestamp, user_id, level, song_id,artist_id, session_id, location, user_agent

The primary key is 'user_id' for joining Songplay Table and Time Table when performing queries.

The song data are divided into 2 dimension tables:

1. Song Table : song_id, title, artist_id, year, duration
2. Artist Table : artist_id, artist_name, artist_location, artist_longitude, artist_latitude

The primary key is 'artist_id' for joining Song Table and Artist Table when performing queries

The ETL pipeline involves:

1. Create the dimension tables in Sparkify schema.
2. Convert JSON data into Pandas dataframes.
3. Parse the data from dataframes to arrays and lists before inserting the data into the respective tables.
4. Timestamp data are further extracted and parsed into hour, day, week_day, month, year  

# Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals. 

The databases allows Sparkify to analyse the streaming behaviour through SQL queries by joining the dimension tables with the primary keys.  For example, joining the Songs Table withd Songplay Tables could allow Sparkify to analyze popular song titles among its users. Yet another example, joining Songplay Table with Users Table could allow Sparkify to analyze popular songs between genders. 

Besides joining of dimension tables, each table by itself could provide analytical values. For example, the Time Table through the extraction of timestamp from log data could provide insights to the days where music streaming traffic is heavy, therefore allowing better management of bandwidth to cater for heavy demand.
