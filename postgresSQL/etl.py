import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    Process JSON data from filepath and insert into song table
    """
    
    # open song file
    df = pd.read_json(filepath, lines = True)
    
    # insert song record
    df_song = df[["song_id", "title", "artist_id", "year", "duration"]]
    df_song = df_song.values
    song_data = df_song.tolist()
    cur.execute(song_table_insert, song_data[0])
    
   
    # insert artist record
    df_artist = df[["artist_id", "artist_name", "artist_location", "artist_longitude", "artist_latitude"]]
    df_artist = df_artist.values
    artist_data = df_artist.tolist()
    cur.execute(artist_table_insert, artist_data[0])


def process_log_file(cur, filepath):
    
    """
    Process JSON data from filepath 
    Parse timestamp to hour, day, weekday, month, year and insert into time table
    Extract and insert user details into user table
    Use song select query to match songs and artists table which contain 'title', 'duration'
    and 'artist_name'
    """
    
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == 'NextSong']

    # convert timestamp column to datetime
    t = df['ts'].astype('datetime64[ms]') 
    
    timestamp = df["ts"]
    hour = pd.Series(t.dt.hour)
    day =  pd.Series(t.dt.day)
    weekday = pd.Series(t.dt.weekday_name)
    month = pd.Series(t.dt.month)
    year = pd.Series(t.dt.year)
    
    # insert time data records
    #time_data = 
    #column_labels = 
    #time_df = pd.DataFrame(list(zip(hour, day, weekday, month, year), columns = ['hour', 'day', 'week_day', 'month', 'year']))
    time_df = pd.DataFrame({'timestamp': timestamp, 'hour': hour, 'day': day, 'weekday': weekday, 'month': month, 'year': year})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Extract JSON from filepath and use process_log_file function
    and process_song_file function
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()