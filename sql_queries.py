import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    staging_event_id BIGINT IDENTITY(0,1),
    artist VARCHAR(100),
    auth VARCHAR(30),
    firstName VARCHAR(50),
    lastName VARCHAR(50),
    song VARCHAR(150),
    gender VARCHAR(2),
    itemInSession INTEGER,
    length DOUBLE PRECISION,
    level VARCHAR(4),
    location VARCHAR(150),
    method VARCHAR(5),
    page VARCHAR(20),
    registration DOUBLE PRECISION,
    sessionId INTEGER,
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR(150),
    userId INTEGER
);
    
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        staging_song_id BIGINT IDENTITY(0, 1),
        num_songs INTEGER,
        artist_id VARCHAR(150),
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR(150),
        artist_name VARCHAR(100),
        song_id VARCHAR(150),
        title VARCHAR(50),
        duration DOUBLE PRECISION,
        year INTEGER
    );
    
""")

songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id BIGINT IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        start_time INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR(4) NOT NULL,
        song_id VARCHAR(150) NOT NULL,
        artist_id VARCHAR(150) NOT NULL,
        session_id INTEGER NOT NULL,
        location VARCHAR(150) NOT NULL,
        user_agent VARCHAR(150) NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        gender CHARACTER(1) NOT NULL,
        level VARCHAR(4) NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR(150) NOT NULL PRIMARY KEY,
        title VARCHAR(50) NOT NULL,
        artist_id VARCHAR(150) NOT NULL,
        year INTEGER NOT NULL,
        duration DOUBLE PRECISION NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR(150) NOT NULL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        location VARCHAR(150) NOT NULL,
        lattitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time INTEGER NOT NULL PRIMARY KEY,
        hour TIME NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month VARCHAR(10) NOT NULL,
        year INTEGER NOT NULL,
        weekday VARCHAR(12) NOT NULL  
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events  (
        artist, auth, firstName, gender,
        itemInSession, lastName, length,
        level, location, method, page,
        registration, sessionId, song,
        status, ts, userAgent, userId
    )
    FROM 's3://udacity-dend/log_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    TIMEFORMAT AS 'epochmillisecs'
    json 's3://udacity-dend/log_json_path.json'
    TRIMBLANKS TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['IAM_ROLE'].get('ARN').strip("'"))

staging_songs_copy = ("""
    COPY staging_songs (
        num_songs, artist_id, artist_latitude,
        artist_longitude, artist_location, artist_name,
        song_id, title, duration, year
    ) 
    FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['IAM_ROLE'].get('ARN').strip("'"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT CAST(EXTRACT(EPOCH FROM se.ts) AS INTEGER) AS start_time, se.userid as user_id, se.level, so.song_id, so.artist_id, se.sessionid as session_id, se.location, se.useragent as user_agent
    FROM staging_events se
    JOIN staging_songs so ON se.song = so.title
""")

user_table_insert = ("""
    INSERT INTO users
    SELECT DISTINCT userid AS user_id, firstname AS first_name, lastname AS last_name, gender AS gender, level as level
    FROM staging_events 
    WHERE page = 'NextSong'
    AND user_id IS NOT NULL 
    AND first_name IS NOT NULL
    AND last_name IS NOT NULL
    AND gender IS NOT NULL
    AND level IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id, artist_name, COALESCE(artist_location, 'Unknown') AS location, artist_latitude as lattitude, artist_longitude as longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time
    SELECT DISTINCT CAST(EXTRACT(EPOCH FROM ts) AS INTEGER) as start_time, ts::timestamp::time as hour, EXTRACT(DAY FROM ts) as day, EXTRACT(WEEK FROM ts) AS week, to_char(ts, 'Month') AS month, CAST(EXTRACT(YEAR FROM ts) AS INTEGER) as year, to_char(ts, 'Day') as weekday
FROM staging_events
WHERE page = 'NextSong'; 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
