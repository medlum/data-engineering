# import configparser
from datetime import datetime
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
# from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pyspark.sql.functions as F


# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("sparkifyETL") \
        .getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return sparki


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    #song_data = input_data + "song_data/A/A/A/*.json"


    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table (song_id, title, artist_id, year, duration)
    songs_table = song_df.select('song_id', 'title','artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'song_table.parquet')

    # extract columns to create artists table (artist_id, name, location, lattitude, longitude)
    artists_table = song_df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    # read in song data to use for songplays table
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.select("userId",
                             "firstName",
                             "lastName",
                             "gender",
                             "level")

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users_log.parquet")

    # read parquet_files
    #spark.read.parquet('users_log.parquet').show()

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000).isoformat())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).date().isoformat())
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.selectExpr('timestamp AS start_time')\
                    .withColumn('hour', F.hour('start_time'))\
                    .withColumn('day', F.dayofmonth('start_time'))\
                    .withColumn('week', F.weekofyear('start_time'))\
                    .withColumn('month', F.month('start_time'))\
                    .withColumn('year', F.year('start_time'))\
                    .withColumn('weekday', F.dayofweek('start_time'))\

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data +  "time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.alias('a').withColumn('songplay_id', monotonically_increasing_id()) \
                        .join(song_df.alias('b'),col('b.artist_name') == col('a.artist')) \
                        .join(time_table.alias('c'),col('c.start_time') == col('a.timestamp')) \
                        .select('songplay_id',
                                'a.ts', 
                                'a.userId' ,
                                'a.level', 
                                'b.song_id', 
                                'b.artist_id', 
                                'a.sessionId', 
                                'a.location', 
                                'a.userAgent',
                                'c.year',
                                'c.month')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays_table.parquet')


def main():
    spark = create_spark_session()
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-user/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
