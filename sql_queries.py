import configparser

# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_times"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist              TEXT,
    auth                TEXT        NOT NULL,
    firstName           TEXT,
    gender              VARCHAR(1),
    itemInSession       INTEGER     NOT NULL,
    lastName            TEXT,
    length              DECIMAL,
    level               VARCHAR(5)  NOT NULL,
    location            TEXT,
    method              VARCHAR(3)  NOT NULL,
    page                VARCHAR(20) NOT NULL,
    registration        DECIMAL,
    sessionId           SMALLINT    NOT NULL,
    song                TEXT,
    status              SMALLINT    NOT NULL,
    ts                  BIGINT      NOT NULL,
    userAgent           TEXT,
    userId              INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs           BIGINT      NOT NULL,
    artist_id           TEXT        NOT NULL,
    artist_latitude     TEXT,
    artist_longitude    TEXT,
    artist_location     TEXT,
    artist_name         TEXT        NOT NULL,
    song_id             TEXT        NOT NULL,
    title               TEXT        NOT NULL,
    duration            DECIMAL     NOT NULL,
    year                INTEGER     NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
    songplay_id         TEXT        PRIMARY KEY,
    start_time          BIGINT      NOT NULL,
    user_id             INTEGER     NOT NULL,
    level               VARCHAR(5)  NOT NULL,
    song_id             TEXT        NOT NULL    SORTKEY,
    artist_id           TEXT        NOT NULL    DISTKEY,
    session_id          SMALLINT    NOT NULL,
    location            TEXT        NOT NULL,
    user_agent          TEXT
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id             INTEGER     PRIMARY KEY UNIQUE,
    first_name          TEXT        NOT NULL,
    last_name           TEXT        NOT NULL,
    gender              VARCHAR(1)  NOT NULL,
    level               VARCHAR(5)  NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id             TEXT        PRIMARY KEY SORTKEY,
    title               TEXT        NOT NULL,
    artist_id           TEXT        NOT NULL    DISTKEY,
    year                INTEGER     NOT NULL,
    duration            DECIMAL     NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id           TEXT      PRIMARY KEY DISTKEY SORTKEY,
    name                TEXT      NOT NULL,
    location            TEXT,
    latitude            TEXT,
    longitude           TEXT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_times
(
    start_time          BIGINT      PRIMARY KEY,
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
json 's3://udacity-dend/log_json_path.json' region 'us-west-2';
""").format("'s3://udacity-dend/log_data'", DWH_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs 
FROM {}
credentials 'aws_iam_role={}'
json 'auto' region 'us-west-2';
""").format("'s3://udacity-dend/song_data'", DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_songplays (songplay_id, start_time, user_id, song_id, artist_id, session_id, user_agent, level, location)
SELECT 
    sessionid || '-' || iteminsession AS songplay_id,
    ts AS start_time,
    userId AS user_id,
    song AS song_id,
    artist AS artist_id,
    sessionId AS session_id,
    userAgent AS user_agent,
    level AS level,
    location AS location
FROM staging_events
WHERE userid IS NOT NULL AND song IS NOT NULL AND artist IS NOT NULL AND location IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO dim_users (user_id, first_name, last_name, gender, level)
SELECT
    DISTINCT(userId) AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender AS gender,
    level AS level
FROM staging_events
WHERE userid IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
SELECT
    DISTINCT(song_id) AS song_id,
    title AS title,
    artist_id AS artist_id,
    year AS year,
    duration AS duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO dim_artists (artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT(artist_id) AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO dim_times (start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT(ts) as start_time,
    EXTRACT(hour from timestamp 'epoch' + ts/1000 * interval '1 second') as hour,
    EXTRACT(day from timestamp 'epoch' + ts/1000 * interval '1 second') as day,
    EXTRACT(week from timestamp 'epoch' + ts/1000 * interval '1 second') as week,
    EXTRACT(month from timestamp 'epoch' + ts/1000 * interval '1 second') as month,
    EXTRACT(year from timestamp 'epoch' + ts/1000 * interval '1 second') as year,
    EXTRACT(weekday from timestamp 'epoch' + ts/1000 * interval '1 second') as weekday
FROM staging_events
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
