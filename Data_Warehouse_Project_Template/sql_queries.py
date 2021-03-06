import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE  IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE  IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE  IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE  IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE  IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
                      artist varchar(max)
                       ,auth text
                       ,firstName text
                       ,gender text
                        ,itemInSession INT8
                        ,lastName text
                        ,length  varchar(50)
                         ,level text
                           ,location text
                            ,method text
                              ,page text
                              ,registration varchar(50)
                                    ,sessionId int
                                     ,song text
                      ,status text
                      ,ts  bigint
                      ,userAgent text
                      ,userId int
                     )
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs

                             (
                              num_songs INTEGER,
                              artist_id VARCHAR(20),
                              artist_latitude NUMERIC(9,6),
                              artist_longitude NUMERIC(9,6),
                              artist_location VARCHAR(256),
                             artist_name VARCHAR(256),
                             song_id VARCHAR(20),
                             title VARCHAR(256),
                            duration NUMERIC(10,6),
                             year INT
                            );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id  int IDENTITY(0,1)
                         ,start_time TIMESTAMP NOT NULL
                         ,user_id int
                         ,artist_name varchar
                         ,level varchar
                         ,song_id varchar
                         ,artist_id varchar
                         ,session_id varchar
                          ,user_agent varchar
                          ,location varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(user_id varchar  PRIMARY KEY
                     ,first_name varchar
                     ,last_name varchar
                     ,gender varchar
                     ,level varchar)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(song_id varchar  PRIMARY KEY
                      ,title varchar
                      ,artist_id varchar
                      ,year int
                      ,duration float)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(artist_id varchar PRIMARY KEY
                       ,name varchar
                       ,location varchar
                       ,latitude varchar
                       ,longitude varchar)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP  PRIMARY KEY
                     ,hour varchar
                     ,day int
                     ,week int
                     ,month int
                     ,year int
                     ,weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from '{}'
 credentials 'aws_iam_role={}'
 region 'us-west-2'

 COMPUPDATE OFF STATUPDATE OFF
 JSON '{}'""").format(config.get('S3','LOG_DATA'),

                        config.get('IAM_ROLE', 'ARN'),

                        config.get('S3','LOG_JSONPATH'))

staging_songs_copy=("""copy staging_songs from '{}'
                         credentials 'aws_iam_role={}'
                         region 'us-west-2'
                         COMPUPDATE OFF STATUPDATE OFF
                         JSON 'auto'""").format(config.get('S3','SONG_DATA'),

                                                config.get('IAM_ROLE', 'ARN'))








# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time ,
                                  user_id,level,
                                  song_id,artist_id,
                                  session_id ,
                                  user_agent ,
                                location )
                             SELECT DISTINCT timestamp 'epoch' + se.ts * interval '0.001 seconds' as  start_time,
                            se.userId,se.level,
                            ss.song_id,ss.artist_id,
                            se.sessionId,se.userAgent,
                            se.location
                            from staging_events se
                            left join staging_songs ss
                            on se.artist=ss.artist_name
                            and
                            se.song=ss.title
                            and
                            se.length=ss.duration
                            WHERE se.page='NextSong' and se.userId is NOT NULL
                            """)

user_table_insert = ("""INSERT INTO users(user_id, first_name,last_name ,gender ,level )
                     select userId, firstName,lastName,gender,level
                     from staging_events where  userId is NOT NULL


""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
                    select   song_id,title,artist_id,year,duration
                    from staging_songs

""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location ,latitude,longitude)
                       select artist_id,artist_name,artist_location,
                       artist_latitude,artist_longitude
                       from  staging_songs

""")

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                    SELECT    DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                    extract(hour from start_time),
                     extract(day from start_time),extract(week from  start_time)
                     ,extract(month from  start_time),extract(year from start_time)
                     ,extract(weekday from start_time) from staging_events

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy,staging_events_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
