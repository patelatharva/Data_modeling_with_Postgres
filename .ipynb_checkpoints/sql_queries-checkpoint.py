# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id SERIAL PRIMARY KEY,
        start_time bigint NOT NULL, 
        user_id text NOT NULL,
        level text,
        song_id text,
        artist_id text,
        session_id int,
        location text, 
        user_agent text
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id text PRIMARY KEY,
        first_name text,
        last_name text,
        gender text,
        level text    
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id text PRIMARY KEY,
        title text,
        artist_id text,
        year int,
        duration numeric
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id text PRIMARY KEY,
        name text,
        location text,
        latitude numeric,
        longitude numeric
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time_id SERIAL PRIMARY KEY,
        start_time timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int
    );
""")

# INSERT RECORDS
#(row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT
    DO NOTHING;
""")
#["userId", "firstName", "lastName", "gender", "level"]
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(user_id)    
    DO UPDATE SET level = excluded.level
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT
    DO NOTHING;
""")
["artist_id", "artist_name", "artist_location", "artist_longitude", "artist_latitude"]
artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, longitude, latitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT
    DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT
    DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT song_id, songs.artist_id AS artist_id 
    FROM songs JOIN artists ON songs.artist_id=artists.artist_id
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]