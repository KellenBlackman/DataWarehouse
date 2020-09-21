import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS fact_songplays"
user_table_drop =           "DROP TABLE IF EXISTS dim_users"
song_table_drop =           "DROP TABLE IF EXISTS dim_songs"
artist_table_drop =         "DROP TABLE IF EXISTS dim_artists"
time_table_drop =           "DROP TABLE IF EXISTS dim_times"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist              STRING,
    auth                STRING      NOT NULL,
    firstName           STRING      NOT NULL,
    gender              VARCHAR(1)  NUL NULL,
    itemInSession       INTEGER     NOT NULL,
    lastName            STRING      NOT NULL,
    length              DOUBLE,
    level               VARCHAR(5)  NOT NULL,
    location            STRING      NOT NULL,
    method              VARCHAR(3)  NOT NULL,
    page                VARCHAR(20) NOT NULL,
    registration        NUMERIC     NOT NULL,
    sessionId           SMALLINT    NOT NULL,
    song                STRING,
    status              SMALLINT    NOT NULL,
    ts                  INTEGER     NOT NULL,
    userAgent           STRING,
    userId              INTEGER     NOT NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs           INTEGER     NOT NULL,
    artist_id           STRING      NOT NULL,
    artist_lattitude    STRING,
    artist_longitude    STRING,
    artist_location     STRING,
    artist_name         STRING      NOT NULL,
    song_id             STRING      NOT NULL,
    title               STRING      NOT NULL,
    duration            DECIMAL     NOT NULL,
    year                INTEGER     NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
    songplay_id         SERIAL      PRIMARY KEY,
    start_time          INTEGER     NOT NULL,
    user_id             INTEGER     NOT NULL,
    level               VARCHAR(5)  NOT NULL,
    song_id             STRING      NOT NULL    SORTKEY,
    artist_id           STRING      NOT NULL    DISTKEY,
    session_id          SMALLINT    NOT NULL,
    location            STRING      NOT NULL,
    user_agent          STRING
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id             INTEGER     PRIMARY KEY,
    first_name          STRING      NOT NULL,
    last_name           STRING      NOT NULL,
    gender              VARCHAR(1)  NUL NULL,
    level               VARCHAR(5)  NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id             STRING      PRIMARY KEY SORTKEY,
    title               STRING      NOT NULL,
    artist_id           STRING      NOT NULL    DISTKEY,
    year                INTEGER     NOT NULL,
    duration            DECIMAL     NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id           STRING      PRIMARY KEY DISTKEY SORTKEY,
    name                STRING      NOT NULL,
    location            STRING      NOT NULL,
    lattitude           STRING,
    longitude           STRING
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time          INTEGER     PRIMARY KEY,
    hour                INTEGER     NOT NULL,
    day                 INTEGER     NOT NULL,
    week                INTEGER     NOT NULL,
    month               INTEGER     NOT NULL,
    year                INTEGER     NOT NULL,
    weekday             INTEGER     NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
credentials 'aws_iam_role={}'
json region 'us-west-2'
""").format("s3://udacity-dend/log_data","arn:aws:iam::878999672007:role/S3AccessRole")

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
credentials 'aws_iam_role={}'
json region 'us-west-2'
""").format("s3://udacity-dend/song_data","arn:aws:iam::878999672007:role/S3AccessRole")

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplays (songplay_id, start_time, user_id, song_id, artist_id, session_id, user_agent, level, location)
SELECT 
    CONCAT(sessionId, itemInSession) AS songplay_id, 
    ts AS start_time,
    userId AS user_id,
    song AS song_id,
    artist AS artist_id,
    sessionId AS session_id,
    userAgent AS user_agent,
    level AS level,
    location AS location
FROM staging_events
ON CONFLICT DO NOTHING
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
SELECT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender AS gender,
    level AS level
FROM staging_events
ON CONFLICT (user_id)
DO UPDATE SET level = excluded.level, first_name = excluded.first_name, last_name = excluded.last_name, gender = excluded.gender
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
SELECT
    song_id AS song_id,
    title AS title,
    artist_id AS artist_id,
    year AS year,
    duration AS duration
FROM staging_songs
ON CONFLICT (song_id)
DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, lattitude, longitude)
SELECT
    artist_id AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_lattitude AS lattitude,
    artist_longitude AS longitude
FROM staging_songs
ON CONFLICT (artist_id)
DO NOTHING
""")

time_table_insert = ("""
INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
SELECT
    ts as start_time,
    EXTRACT(hour from timestamp ts),
    EXTRACT(day from timestamp ts),
    EXTRACT(week from timestamp ts),
    EXTRACT(month from timestamp ts),
    EXTRACT(year from timestamp ts),
    EXTRACT(weekday from timestamp ts)
FROM staging_events
ON CONFLICT (ts)
DO NOTHING
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
