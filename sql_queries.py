import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts VARCHAR,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(2560),
    num_songs INT,
    artist_id VARCHAR(2560),
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR(2560),
    artist_name VARCHAR(2560),
    title VARCHAR(2560),
    duration DECIMAL,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
    user_id VARCHAR NOT NULL REFERENCES users(user_id),
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL REFERENCES songs(song_id),
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id),
    session_id VARCHAR NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender VARCHAR, 
    level VARCHAR NOT NULL 
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR(2560) NOT NULL, 
    artist_id VARCHAR NOT NULL REFERENCES artists(artist_id), 
    year INT, 
    duration DECIMAL NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR(2560) NOT NULL, 
    location VARCHAR(2560), 
    latitude DECIMAL, 
    longitude DECIMAL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON {} 
    REGION 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    JSON 'auto'
    REGION 'us-west-2'
    COMPUPDATE OFF STATUPDATE OFF;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent )
    (
        SELECT DISTINCT
            timestamp 'epoch' + CAST(ste.ts AS BIGINT)/1000 * interval '1 second' as start_time,
            ste.userid as user_id,
            ste.level as level,
            sts.song_id as song_id,
            sts.artist_id as artist_id,
            ste.sessionId as session_id,
            ste.location as location,
            ste.userAgent as user_agent
        FROM staging_events ste
        JOIN staging_songs sts
            ON TRIM(ste.artist) = TRIM(sts.artist_name) and TRIM(ste.song) = TRIM(sts.title)
        WHERE ste.page = 'NextSong'
    )
""")

user_table_insert = ("""
    INSERT INTO users (
        SELECT DISTINCT userId, firstName, lastName, gender, level
        FROM staging_events
        WHERE page = 'NextSong'
    );
""")

song_table_insert = ("""
    INSERT INTO songs (
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
    );

""")

artist_table_insert = ("""
    INSERT INTO artists (
	    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
        FROM staging_songs
    );
""")

time_table_insert = ("""
    INSERT INTO time (
        SELECT DISTINCT timestamp as start_time, 
        DATEPART(HOUR, timestamp) as hour,
        DATEPART(DAY, timestamp) as day,
        DATEPART(WEEK, timestamp) as week,
        DATEPART(MONTH, timestamp) as month,
        DATEPART(YEAR, timestamp) as year,
        DATEPART(DOW, timestamp) as weekday
        FROM 
        (
            SELECT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' as timestamp
            FROM staging_events
            WHERE page = 'NextSong'
        )
    );
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, songplay_table_insert, user_table_insert, song_table_insert, time_table_insert]
