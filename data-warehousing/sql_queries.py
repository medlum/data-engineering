import configparser

# 

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE STAGING TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events_table (
                                                artist        varchar,
                                                auth          varchar,
                                                firstName     varchar,
                                                gender        varchar,
                                                itemInSession int,
                                                lastName      varchar,
                                                length        double precision,
                                                level         varchar,
                                                location      varchar,
                                                method        varchar,
                                                page          varchar,
                                                registration  decimal,
                                                sessionId     double precision,
                                                song          varchar,
                                                status        int,
                                                ts            bigint, 
                                                userAgent     varchar,
                                                userId        varchar);
                            """)

staging_songs_table_create= ("""
                                CREATE TABLE IF NOT EXISTS staging_songs_table(
                                artist_id         varchar,
                                artist_latitude   double precision,
                                artist_location   varchar,
                                artist_longitude  double precision,
                                artist_name       varchar,
                                duration          double precision,
                                num_songs         int, 
                                song_id           varchar,
                                title             varchar,
                                year              int) ;                         
                                """)


# CREATE REDSHIFT TABLES
songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplay_table(
                            songplay_id    bigint identity (0,1) primary key,
                            start_time     timestamp        not null references time_table,
                            user_id        varchar          not null references user_table,
                            level          varchar          not null,
                            song_id        varchar                   references song_table,
                            artist_id      varchar                   references artist_table,
                            session_id     double precision not null,
                            location       varchar          not null,
                            user_agent     varchar          not null) diststyle all;
                            """)

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS user_table(
                        user_id        varchar primary key,
                        first_name     varchar,
                        last_name      varchar,
                        gender         varchar,
                        level          varchar) diststyle all;
                        """)

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS song_table(
                        song_id       varchar primary key,
                        title         varchar,
                        artist_id     varchar, 
                        year          int,
                        duration      double precision)  diststyle all;
                        """)

artist_table_create = ("""
                        CREATE TABLE IF NOT EXISTS artist_table(
                        artist_id    varchar primary key,
                        name         varchar,
                        location     varchar, 
                        latitude     double precision, 
                        longitude    double precision)  diststyle all;
                        """)


time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS time_table(
                        start_time  timestamp primary key, 
                        hour        int, 
                        day         int, 
                        week        int, 
                        month       int, 
                        year        int, 
                        weekday     varchar)  diststyle all;
                        """)


# STAGING TABLES
staging_events_copy = (""" copy staging_events_table from {} iam_role {} compupdate off region 'us-west-2' json {} ; """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = (""" copy staging_songs_table from {} iam_role {} compupdate off region 'us-west-2' json 'auto'; """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))



# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplay_table (
                            start_time, user_id, level,
                            song_id, artist_id, session_id, 
                            location, user_agent)
                                                        
                            SELECT 
                            events.timestamp         AS start_time,
                            events.userid             AS user_id,
                            events.level               AS level,
                            songs.song_id             AS song_id,
                            songs.artist_id           AS artist_id,
                            events.sessionid          AS session_id,
                            events.location            AS location,
                            events.useragent          AS user_agent
                            
                            FROM
                                (
                                    SELECT 
                                        userid, 
                                        level, 
                                        sessionid, 
                                        location, 
                                        useragent, 
                                        artist, 
                                        song, 
                                        length,
                                        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS timestamp
                                        FROM staging_events_table WHERE page = 'NextSong'
                                            
                                 ) as events
                            
                            LEFT JOIN staging_songs_table AS songs 
                            ON (songs.artist_name = events.artist)
                            AND (songs.title = events.song)
                            AND (songs.duration = events.length);
                        """)

user_table_insert = ("""
                        INSERT INTO user_table (user_id, first_name, last_name, gender, level)
                        
                        SELECT 
                        DISTINCT(userid)  AS user_id,
                        firstName          AS first_name,
                        lastName           AS last_name,
                        gender             AS gender,
                        level              AS level
                        
                        FROM staging_events_table
                        WHERE userid IS NOT NULL AND page = 'NextSong';
                        """)

song_table_insert = ("""
                        INSERT INTO song_table (song_id, title, artist_id, year, duration)
                        
                        SELECT 
                        song_id   AS song_id,
                        title     AS title,
                        artist_id AS artist_id,
                        year      AS year,
                        duration  AS duration
                        
                        FROM staging_songs_table;

                      """)

artist_table_insert = ("""
                        INSERT INTO artist_table(artist_id, name, location, latitude, longitude)
                        
                        SELECT 
                        DISTINCT(artist_id)   AS artist_id,
                        artist_name           AS name,
                        artist_location       AS location,
                        artist_latitude       AS latitude,
                        artist_longitude      AS longitude
                        
                        FROM staging_songs_table;                        
                        """)

time_table_insert = ("""
                        INSERT INTO time_table (start_time,hour,day,week,month,year,weekday)
                        
                        SELECT DISTINCT(a.timestamp) AS start_time,
                        EXTRACT (HOUR FROM a.timestamp) as hour,
                        EXTRACT (DAY FROM a.timestamp) as day,
                        EXTRACT (WEEK FROM a.timestamp) as week,
                        EXTRACT (MONTH FROM a.timestamp) as month,
                        EXTRACT (YEAR FROM a.timestamp) as year,
                        EXTRACT (DOW FROM a.timestamp) as weekday
                        
                        FROM 
                            (SELECT 
                            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS timestamp
                            FROM staging_events_table WHERE page = 'NextSong') a;
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,  songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
