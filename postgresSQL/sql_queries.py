print("Loading module...")


# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

## CREATE TABLES
songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays(songtable_id serial primary key, timestamp float, user_id varchar, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);")

song_table_create = ("CREATE TABLE IF NOT EXISTS songs(song_id varchar PRIMARY KEY, title varchar, \
artist_id varchar , year int NOT NULL, duration float NOT NULL);")

artist_table_create = ("CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY, artist_name varchar, artist_location varchar NOT NULL, artist_longitude float, artist_latitude float);")

time_table_create = ("CREATE TABLE IF NOT EXISTS time(start_time float primary key, hour int, day int, weekday varchar, month int, year int);")

user_table_create = ("CREATE TABLE IF NOT EXISTS users (userId int PRIMARY KEY, firstName varchar, lastName varchar, gender varchar NOT NULL, level varchar NOT NULL);")

## INSERT DATA

songplay_table_insert = ("INSERT INTO songplays(timestamp, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

song_table_insert = ("INSERT INTO songs (song_id, title, artist_id, year, duration)\
VALUES (%s, %s, %s, %s, %s)  ON CONFLICT (song_id) DO NOTHING")

artist_table_insert = ("INSERT INTO artists(artist_id, artist_name, artist_location, artist_longitude , artist_latitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO NOTHING")

time_table_insert = ("INSERT INTO time(start_time, hour, day, weekday, month, year) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(start_time) DO NOTHING")

user_table_insert = ("INSERT INTO users (userId, firstName, lastName, gender, level) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (userId) DO UPDATE SET level = EXCLUDED.level")

# FIND SONGS

song_select = "select songs.song_id, artists.artist_id \
from songs join artists on songs.artist_id = artists.artist_id \
where songs.title = %s and artists.artist_name = %s and songs.duration = %s"


## CREATE AND DROP TABLES QUERIES

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]