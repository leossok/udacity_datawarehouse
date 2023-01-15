import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplays_table_drop = "DROP TABLE IF EXISTS songplays;"
users_table_drop = "DROP TABLE IF EXISTS users;"
songs_table_drop = "DROP TABLE IF EXISTS songs;"
artists_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
artist text ,
auth text,
firstName text,
gender text,
itemInSession int,
lastName text,
length double precision,
level text,
location text,
method text,
page text,
registration timestamp,
sessionId int,
song text,
status int,
ts timestamp,
userAgent text,
userId text
);

"""

# Example data
# {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "",
#  "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
song_id text primary key,
num_songs int,
artist_id text,
artist_latitude double precision,
artist_longitude double precision,
artist_location text,
artist_name text,
title text,
duration double precision,
year int
);
"""

songplays_table_create = """
CREATE TABLE IF NOT EXISTS songplays (
songplay_id int identity(0, 1) PRIMARY KEY,
start_time timestamp,
user_id varchar,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar
);
"""

# user_id, first_name, last_name, gender, level
users_table_create = """
CREATE TABLE IF NOT EXISTS users (
user_id varchar PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
);
"""

songs_table_create = """
CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration double precision
);
"""

artists_table_create = """
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude double precision,
longitude double precision
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS time (
start_time timestamp PRIMARY KEY,
hour int,
day int,
week int,
year int,
weekday int
);
"""

# STAGING TABLES

# ref: https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
# ref: https://docs.aws.amazon.com/redshift/latest/dg/r_DATEFORMAT_and_TIMEFORMAT_strings.html
# ref: https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html
staging_events_copy = (
    """
COPY staging_events from '{}'
credentials 'aws_iam_role={}'
FORMAT AS JSON '{}'
REGION 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
"""
).format(
    "s3://udacity-dend/log_data/2018/11/2018-11",
    config["IAM_ROLE"].get("ARN").replace("'", ""),
    "s3://udacity-dend/log_json_path.json",
)

staging_songs_copy = (
    """
COPY staging_songs from '{}'
credentials 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2'
"""
).format(
    # "s3://udacity-dend/song_data", # I use 2 node cluster, full dataset take me so long (https://knowledge.udacity.com/questions/947620)
    "s3://udacity-dend/song_data/A",  # use smaller dataset
    config["IAM_ROLE"].get("ARN").replace("'", ""),
)

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent
FROM staging_events as e
JOIN staging_songs as s
    ON e.song = s.title
    AND e.artist = s.artist_name
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId is NOT NULL
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id is NOT NULL
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id is NOT NULL
"""

time_table_insert = """
INSERT INTO time (start_time, hour, day, week, year, weekday)
SELECT DISTINCT ts, EXTRACT(hour from ts), EXTRACT(day from ts), EXTRACT(week from ts), EXTRACT(year from ts), EXTRACT(weekday from ts)
FROM staging_events
WHERE ts is NOT NULL
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplays_table_create,
    users_table_create,
    songs_table_create,
    artists_table_create,
    time_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplays_table_drop,
    users_table_drop,
    songs_table_drop,
    artists_table_drop,
    time_table_drop,
]

# copy_table_queries = [staging_events_copy]
# copy_table_queries = [staging_songs_copy]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

drop_staging_table_queries = [staging_events_table_drop, staging_songs_table_drop]
drop_table_queries = [songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplays_table_create,
    users_table_create,
    songs_table_create,
    artists_table_create,
    time_table_create,
]
