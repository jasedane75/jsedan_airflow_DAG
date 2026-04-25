class SqlQueries:
       
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    esquema =  ("""

              DROP TABLE IF EXISTS staging_events;
              CREATE TABLE staging_events(
                     artist        VARCHAR,
                     auth          VARCHAR,
                     firstName     VARCHAR,
                     gender        CHAR,
                     itemInSession INTEGER,
                     lastName      VARCHAR,
                     length        FLOAT,
                     level         VARCHAR,
                     location      VARCHAR,
                     method        VARCHAR,
                     page          VARCHAR,
                     registration  VARCHAR,
                     sessionId     INTEGER,
                     song          VARCHAR,
                     status        INTEGER,
                     ts            BIGINT, 
                     userAgent     VARCHAR, 
                     userId        INTEGER
              );
              
              DROP TABLE IF EXISTS staging_songs;
              CREATE TABLE staging_songs(
                     num_songs         INTEGER,
                     artist_id         VARCHAR,
                     artist_latitude   VARCHAR,
                     artist_longitude  VARCHAR,
                     artist_location   VARCHAR,
                     artist_name       VARCHAR,
                     song_id           VARCHAR,
                     title             VARCHAR,
                     duration          FLOAT,
                     year              INTEGER
              );

              DROP TABLE IF EXISTS songplays;
              CREATE TABLE songplays(
                     songplay_id   VARCHAR PRIMARY KEY,
                     start_time    TIMESTAMP NOT NULL,
                     user_id       INTEGER NOT NULL,
                     level         VARCHAR,
                     song_id       VARCHAR,
                     artist_id     VARCHAR,
                     session_id    INTEGER,
                     location      VARCHAR,
                     user_agent    VARCHAR);   

              DROP TABLE IF EXISTS users;
              CREATE TABLE users(
                     user_id       VARCHAR PRIMARY KEY,
                     first_name    VARCHAR,
                     last_name     VARCHAR,
                     gender        VARCHAR,
                     level         VARCHAR);

              DROP TABLE IF EXISTS songs;
              CREATE TABLE songs(
                     song_id     VARCHAR PRIMARY KEY,
                     title       VARCHAR NOT NULL,
                     artist_id   VARCHAR NOT NULL,
                     year        INTEGER,
                     duration    FLOAT);

              DROP TABLE IF EXISTS artists;
              CREATE TABLE artists(
                     artist_id  VARCHAR PRIMARY KEY,
                     name       VARCHAR NOT NULL,
                     location   VARCHAR,
                     latitude   VARCHAR,
                     longitude  VARCHAR);

              DROP TABLE IF EXISTS time;
              CREATE TABLE time(
                     start_time TIMESTAMP sortkey PRIMARY KEY,
                     hour       INTEGER NOT NULL,
                     day        INTEGER NOT NULL,
                     week       INTEGER NOT NULL,
                     month      INTEGER NOT NULL,
                     year       INTEGER NOT NULL,
                     weekday    INTEGER NOT NULL);

       """)
