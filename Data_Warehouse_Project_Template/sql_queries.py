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
                      artist varchar
                      ,status varchar
                      ,userAgent varchar
                      ,song varchar
                      ,sessionId int
                      ,registration varchar
                      ,page varchar
                      ,method varchar
                      ,location varchar
                      ,length float
                      ,auth varchar
                      ,itemInSession int
                      ,userId varchar
                     ,firstName varchar
                     ,lastName varchar
                     ,gender varchar
                     ,level varchar
                     ,ts time)

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
                      song_id varchar
                      ,title varchar

                      ,year int
                      ,duration float
                      ,artist_id varchar
                       ,name varchar
                       ,location varchar
                       ,latitude varchar
                       ,longitude varchar)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id  int IDENTITY(0,1)
                         ,start_time time NOT NULL
                         ,user_id int NOT NULL
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
CREATE TABLE IF NOT EXISTS time (start_time time PRIMARY KEY
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


staging_songs_copy =("""copy staging_songs from '{}'
 JSON '{}'""").format(config.get('S3','SONG_DATA'),

                        config.get('IAM_ROLE', 'ARN'),

                        config.get('S3','LOG_JSONPATH'))


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(start_time ,
                                  user_id,level,
                                  song_id,artist_id,
                                  session_id ,
                                  user_agent ,
                                location )
                            select  extract (time,se.ts),
                            se.userId,se.level,
                            ss.song_id,ss.artist_id,
                            se.sessionId,se.userAgent,
                            se.location
                            from staging_events se
                            join staging_songs ss
                            on se.artist=ss.artist_name
                            and
                            se.song=ss.title
                            and
                            se.length=ss.duration
                            where WHERE se.page='NextSong'
                            """)

user_table_insert = ("""INSERT INTO users(user_id, first_name,last_name ,gender ,level )
                     select userId, firstName,lastName,gender,level
                     from staging_events
                     ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level;

""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
                    select   song_id,title,artist_id,year,duration
                    from staging_songs
                     ON CONFLICT (song_id)  DO NOTHING ;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location ,latitude,longitude)
                       select artist_id,name,location,latitude,longitude
                       from  staging_songs
                       ON CONFLICT (artist_id)  DO UPDATE SET location=EXCLUDED.location
""")

time_table_insert = ("""INSERT INTO time (start_time,hour,day,week,month,year,weekday)
                     select   extract (time,ts), extract(hour,ts),
                     extract(day,ts),extract(week,ts)
                     ,extract(month,ts),extract(year,ts)
                     ,extract(weekday,ts)
                     ON CONFLICT (start_time) DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
