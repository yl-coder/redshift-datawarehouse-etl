import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH") 
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stg_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stg_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fct_songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("create table if not exists stg_events\
(\
    artist varchar,\
    auth varchar,\
    firstName varchar,\
    gender varchar,\
    itemInSession integer,\
    lastName varchar,\
    length float,\
    level varchar,\
    location varchar,\
    method varchar,\
    page varchar,\
    registration bigint,\
    sessionId integer,\
    song varchar,\
    status integer,\
    ts bigint,\
    userAgent varchar,\
    userId integer\
);")

staging_songs_table_create = ("create table if not exists stg_songs\
(\
    song_id varchar,\
    num_songs integer,\
    title varchar,\
    artist_name varchar,\
    artist_latitude float,\
    year integer,\
    duration float,\
    artist_id varchar,\
    artist_longitude float,\
    artist_location varchar\
);")


songplay_table_create = ("create table if not exists fct_songplays\
(\
	songplay_id bigint identity(0, 1),\
	start_time bigint not null,\
	user_id integer not null,\
	level varchar,\
	song_id varchar not null,\
	artist_id varchar not null,\
	session_id varchar,\
	location varchar,\
	user_agent varchar,\
    primary key(songplay_id)\
    );\
")

user_table_create = ("create table if not exists users\
(\
	user_id integer,\
	first_name varchar,\
	last_name varchar,\
	gender varchar,\
	level varchar,\
    primary key(user_id)\
);\
")

song_table_create = ("create table if not exists songs\
(\
	song_id varchar,\
	title varchar,\
	artist_id varchar not null,\
	year int,\
	duration decimal,\
    primary key(song_id)\
);\
")

artist_table_create = ("create table if not exists artists\
(\
	artist_id varchar,\
	name varchar,\
	location varchar,\
	latitude float,\
	longitude float,\
    primary key(artist_id)\
);\
")

time_table_create = ("create table time\
(\
	start_time bigint,\
	hour int,\
	day int,\
	week int,\
	month int,\
	year int,\
	weekday int\
);\
")


# STAGING TABLES

staging_events_copy = ("copy stg_events from {} credentials 'aws_iam_role={}' format as json {} ").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("copy stg_songs from {} credentials 'aws_iam_role={}' format as json 'auto' ").format(SONG_DATA, ARN)

# FINAL TABLES
songplay_table_insert = ("INSERT INTO fct_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT DISTINCT e.ts as start_time, e.userId as user_id, e.level, s.song_id as song_id, s.artist_id as artist_id, e.sessionId as session_id, e.location as location_id, e.userAgent as user_agent FROM stg_events e JOIN stg_songs s on e.song = s.title and e.artist = s.artist_name WHERE e.userId IS NOT NULL AND e.page='NextSong'")

user_table_insert = ("INSERT INTO users(user_id, first_name, last_name, gender, level) SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level from stg_events WHERE user_id IS NOT NULL AND page='NextSong'")

song_table_insert = ("INSERT INTO songs(song_id, title, artist_id, year, duration) SELECT distinct song_id, title, artist_id, year, duration from stg_songs")

artist_table_insert = ("INSERT INTO artists(artist_id, name, location, latitude, longitude) SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from stg_songs")

time_table_insert = ("INSERT INTO time(start_time, hour, day, week, month, year, weekday) SELECT start_time, \
EXTRACT(hour from timestamp 'epoch' + start_time/1000 * interval '1 second') as hour, \
EXTRACT(day from timestamp 'epoch' + start_time/1000 * interval '1 second') as day, \
EXTRACT(week from timestamp 'epoch' + start_time/1000 * interval '1 second') as week, \
EXTRACT(month from timestamp 'epoch' + start_time/1000 * interval '1 second') as month,\
EXTRACT(year from timestamp 'epoch' + start_time/1000 * interval '1 second') as year, \
EXTRACT(weekday from timestamp 'epoch' + start_time/1000 * interval '1 second') as weekday \
FROM fct_songplays WHERE start_time is not null")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
