
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS song_play;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;


CREATE TABLE IF NOT EXISTS staging_events(
          artist                    VARCHAR,
          auth                      VARCHAR,
          first_name                VARCHAR,
          gender                    CHAR,
          item_in_session           INTEGER,
          last_name                 VARCHAR,
          length                    FLOAT,
          level                     VARCHAR,
          location                  VARCHAR,
          method                    VARCHAR,
          page                      VARCHAR,
          registration              FLOAT,
          session_id                INTEGER NOT NULL SORTKEY DISTKEY,
          song                      VARCHAR,
          status                    INTEGER,
          ts                        BIGINT,
          user_agent                VARCHAR,
          user_id                   INTEGER
);


 CREATE TABLE IF NOT EXISTS staging_songs(
           num_songs                INTEGER,
           artist_id                VARCHAR,
           artist_latitude          FLOAT, 
           artist_longitude         FLOAT,
           artist_location          VARCHAR,
           artist_name              VARCHAR SORTKEY DISTKEY,
           song_id                  VARCHAR,
           title                    VARCHAR,
           duration                 FLOAT,
           year                     INTEGER
           );   



CREATE TABLE IF NOT EXISTS song_play(
           songplay_id              INTEGER IDENTITY (0,1) PRIMARY KEY,
           start_time               TIMESTAMP NOT NULL,
           user_id                  INTEGER NOT NULL, 
           level                    VARCHAR,
           song_id                  VARCHAR,
           artist_id                VARCHAR,
           session_id               INTEGER,
           location                 VARCHAR,
           user_agent               VARCHAR
           );  



CREATE TABLE IF NOT EXISTS users(
           user_id                  INTEGER PRIMARY KEY SORTKEY,
           first_name               VARCHAR,
           last_name                VARCHAR, 
           gender                   CHAR,
           level                    VARCHAR
           );


CREATE TABLE IF NOT EXISTS songs(
           song_id                  VARCHAR PRIMARY KEY SORTKEY,
           title                    VARCHAR,
           artist_id                VARCHAR DISTKEY, 
           year                     INTEGER,
           duration                 FLOAT
           );



CREATE TABLE IF NOT EXISTS artists(
           artist_id               VARCHAR NOT NULL PRIMARY KEY SORTKEY,
           name                    VARCHAR,
           location                VARCHAR, 
           latitude                FLOAT,
           longitude               FLOAT
           );


CREATE TABLE IF NOT EXISTS time(
           start_time              TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
           hour                    INTEGER,
           day                     INTEGER,
           week                    INTEGER,
           month                   INTEGER,
           year                    INTEGER,
           weekday                 INTEGER
           );
