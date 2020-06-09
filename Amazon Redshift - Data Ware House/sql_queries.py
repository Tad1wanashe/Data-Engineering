import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF Exists staging_events"
staging_songs_table_drop = "DROP TABLE IF Exists staging_songs"
songplay_table_drop = "DROP TABLE IF Exists songplay"
user_table_drop = "DROP TABLE IF Exists users"
song_table_drop = "DROP TABLE IF Exists songs"
artist_table_drop = "DROP TABLE IF Exists artists"
time_table_drop = "DROP TABLE IF Exists time"

# CREATE TABLES

staging_events_table_create= (""" Create Table staging_events(
                             artist            VARCHAR,
                             auth              VARCHAR,
                             firstName         VARCHAR,
                             gender            VARCHAR,
                             itemInSession     INTEGER,
                             lastname          VARCHAR,
                             length            FLOAT,
                             level             VARCHAR,
                             location          VARCHAR,
                             method            VARCHAR,
                             page              VARCHAR,
                             registration      FLOAT,
                             sessionId         INTEGER,
                             song              VARCHAR,
                             status            INTEGER,
                             ts                BIGINT,
                             userAgent         VARCHAR,
                             userId            INTEGER 
)
""")

staging_songs_table_create = (""" Create Table staging_songs(
                             num_songs         INTEGER,
                             artist_id         VARCHAR,
                             artist_latitude   FLOAT,
                             artist_longitude  FLOAT,
                             artist_location   VARCHAR,
                             artist_name       VARCHAR,
                             song_id           VARCHAR,
                             title             VARCHAR,
                             duration          FLOAT,
                             year              INTEGER
)
""")

songplay_table_create = (""" Create Table songplay(
                        songplay_id      INTEGER      IDENTITY(0,1)    PRIMARY KEY,
                        start_time       TIMESTAMP NOT NULL SORTKEY DISTKEY,
                        user_id          INTEGER      NOT NULL,
                        level            VARCHAR      NOT NULL,
                        song_id          VARCHAR      NOT NULL,
                        artist_id        VARCHAR      NOT NULL,
                        session_id       INTEGER      NOT NULL,
                        location         VARCHAR      NOT NULL,
                        user_agent       VARCHAR      NOT NULL
)
""")

user_table_create = (""" Create Table users(
                    user_id           INTEGER      NOT NULL SORTKEY  PRIMARY KEY,
                    first_name        VARCHAR      NOT NULL,
                    last_name         VARCHAR      NOT NULL,
                    gender            VARCHAR      NOT NULL,
                    level             VARCHAR      NOT NULL
)
""")

song_table_create = (""" Create Table songs(
                    song_id           VARCHAR      NOT NULL SORTKEY PRIMARY KEY,
                    title             VARCHAR      NOT NULL,
                    artist_id         VARCHAR      NOT NULL,
                    year              INTEGER,
                    duration          float        NOT NULL                   
)
""")

artist_table_create = (""" Create Table artists(
                      artist_id       VARCHAR      NOT NULL SORTKEY PRIMARY KEY,
                      name            VARCHAR      NOT NULL,
                      location        VARCHAR,
                      lattitude       VARCHAR,
                      longitude       VARCHAR
)
""")

time_table_create = (""" Create Table time(
                    start_time       timestamp    NOT NULL SORTKEY DISTKEY PRIMARY KEY,
                    hour             INTEGER,
                    day              INTEGER,
                    week             INTEGER,
                    month            INTEGER,
                    year             INTEGER,
                    weekday          varchar
)
""")

# STAGING TABLES

staging_events_copy = ("""  copy staging_events 
                            from {}
                            iam_role {}
                            json {}
                        """).format(LOG_DATA, IAM, LOG_JSONPATH)
staging_songs_copy = (""" copy staging_songs
                            from {}
                            iam_role {}
                            json 'auto'
                      """).format(SONG_DATA, IAM)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second'  AS start_time, 
            se.userId                                     AS user_id, 
            se.level                                      AS level, 
            ss.song_id                                    AS song_id, 
            ss.artist_id                                  AS artist_id, 
            se.sessionId                                  AS session_id, 
            se.location                                   AS location, 
            se.userAgent                                  AS user_agent
    FROM staging_events se
    JOIN staging_songs  ss   ON (se.song = ss.title AND se.artist = ss.artist_name)
    AND se.page  =  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS lattitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  start_time                                           AS start_time,
            EXTRACT(hour FROM start_time)                        AS hour,
            EXTRACT(day FROM start_time)                         AS day,
            EXTRACT(week FROM start_time)                        AS week,
            EXTRACT(month FROM start_time)                       AS month,
            EXTRACT(year FROM start_time)                        AS year,
            EXTRACT(dayofweek FROM start_time)                   as weekday
    FROM songplay;
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
