import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
    This function processes song file. The filepath of the song is passed in as an argument.
    It extracts the information about the song from that file and loads it into songs table.
    It also extracts information about the artist of the song from that file and loads it into artists table.

    INPUTS: 
    * cur the cursor variable of database
    * filepath path to file containing song and artist data
"""
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id','year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_longitude", "artist_latitude"]].values[0]
    cur.execute(artist_table_insert, artist_data)

"""
    This function processes the log file containing data about users' activity for e.g. playing a song.
    From each line in the file, it extracts data about user's activity. It specifically it looks for action where user plays next song.
    It then extracts timestamp where the user started playing the song.
    It transforms the timestamp by getting separate attributes like hour, day, week, month, year and weekday of that timestamp and loads that data along with timestamp into table named 'time'.
    It extracts attributes about user like user ID, first name, last name, gender and level of usage i.e. free or paid from the log record and loads it into tabled named 'users'.
    Then it finds song ID and artist ID from the log entry of song played by user.
    It then loads data related to the song played by user by inserting a row containing attributes like timestamp, user ID, level of usage, song ID, artist ID, session ID, artist's location, user's browser agent into table named 'songplays'.

    INPUTS:
    * cur the cursor variable of database
    * filepath path to file containing log entries about user activities for e.g. playing next song
"""
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")
    
    # insert time data records
    time_data = pd.concat([t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday], axis=1)
    column_labels = ["timestamp", "hour", "day", "week", "month", "year", "weekday"]
    time_data.columns = column_labels
    time_df = time_data 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        timestamp = pd.to_datetime(row.ts, unit='ms').to_pydatetime()
        songplay_data = (timestamp, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

"""

    This function walks through all the files present in the directory located at the filepath passed as an argument.
    It also accepts cursor variable to database, connection to database as arguments and custom function responsible to process the files as arguments.
    For each file in the directory, it calls the function passed in the arguments by passing the cursor to database and path of the file as arguments.
    Finally it commits to the database using the database connection received as argument.
    INPUTS:
    * cur cursor variable of database
    * conn connection to database
    * filepath path to directory containing files to be processed
    * func function rensponsible to process the files present under directory located at 'filepath'
"""
def process_data(cur, conn, filepath, func):
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