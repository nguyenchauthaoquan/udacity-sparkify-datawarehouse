import configparser


# CONFIG
config = configparser.ConfigParser()
config.read(['./configurations/dwh.cfg'])

# GETTING ALL CONFIG VARIABLES

# LOGS
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM", "ARN_IAM_ROLE")
REGION = config.get("CLUSTER", "REGION")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(255),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(255),
    length FLOAT,
    level VARCHAR(255),
    location VARCHAR(255),
    method VARCHAR(255),
    page VARCHAR(255),
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR(255),
    status INTEGER,
    ts TIMESTAMP,
    userAgent VARCHAR(255),
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id VARCHAR(255),
    artist_latitude FLOAT,
    artist_location VARCHAR(255),
    artist_longitude FLOAT,
    artist_name VARCHAR(255),
    duration FLOAT,
    num_songs INTEGER,
    song_id VARCHAR(255),
    title VARCHAR(255),
    year INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) DISTKEY PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    level VARCHAR(255),
    song_id VARCHAR(255),
    artist_id VARCHAR(255),
    session_id INTEGER,
    location VARCHAR(255),
    user_agent VARCHAR(255),
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY(start_time) REFERENCES time(start_time)
)
COMPOUND SORTKEY(songplay_id, user_id, song_id, artist_id, start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL DISTKEY SORTKEY PRIMARY KEY, 
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL, 
    gender CHAR(1) NOT NULL, 
    level VARCHAR(255) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
     song_id VARCHAR(255) NOT NULL DISTKEY SORTKEY PRIMARY KEY,
     title VARCHAR(255) NOT NULL,
     artist_id VARCHAR(255) NOT NULL,
     year VARCHAR(255) NOT NULL,
     duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(255) NOT NULL DISTKEY SORTKEY PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL DISTKEY SORTKEY PRIMARY KEY, 
    hour INTEGER NOT NULL, 
    day INTEGER NOT NULL, 
    week INTEGER NOT NULL, 
    month INTEGER NOT NULL, 
    year INTEGER NOT NULL, 
    weekday VARCHAR(20) NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region '{}'
TIMEFORMAT as 'epochmillisecs'
FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE, REGION, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE OFF region '{}'
FORMAT AS JSON 'auto';
""").format(SONG_DATA, IAM_ROLE, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT (se.ts) AS start_time,
       se.userId AS user_id,
       se.level AS level,
       ss.song_id AS song_id,
       ss.artist_id AS artist_id,
       se.sessionId AS session_id,
       se.location AS location,
       se.userAgent AS user_agent
FROM staging_events AS se
JOIN staging_songs AS ss ON (se.song = ss.title AND se.artist = ss.artist_name)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT (se.userId) AS user_id,
       se.firstName AS first_name,
       se.lastName AS last_name,
       se.gender AS gender,
       se.level AS level
FROM staging_events AS se
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT (ss.song_id) AS song_id,
       ss.title AS title,
       ss.artist_id AS artist_id,
       ss.year AS year,
       ss.duration AS duration
FROM staging_songs AS ss
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT (ss.artist_id) AS artist_id,
                ss.artist_name AS name,
                ss.artist_location AS location,
                ss.artist_latitude AS latitude,
                ss.artist_longitude AS longitude
FROM staging_songs AS ss
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT (sp.start_time) AS start_time,
       EXTRACT(HOUR from sp.start_time),
       EXTRACT(DAY from sp.start_time),
       EXTRACT(WEEK from sp.start_time),
       EXTRACT(MONTH from sp.start_time),
       EXTRACT(YEAR from sp.start_time),
       EXTRACT(WEEKDAY from sp.start_time)
FROM songplays AS sp;
""")

# QUERY LISTS

create_table_queries = [
                        staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create,
                       ]
drop_table_queries = [
                        staging_events_table_drop,
                        staging_songs_table_drop,
                        songplay_table_drop,
                        user_table_drop,
                        song_table_drop,
                        artist_table_drop,
                        time_table_drop
                     ]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
                        songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert
                      ]
