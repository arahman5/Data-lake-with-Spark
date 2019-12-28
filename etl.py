import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID'] # Load in the Key details from AWS Config file
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY'] # Load in the Key details from AWS Config file

# Initialize the Spark Session or get an existing one
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

## NOTE: I created a separate view for each table just to keep the SQL Statements more concise and easy to read and to reduce typos.

def process_song_data(spark, input_data, output_data):
    """
    This function loads song_data from S3 and processes it by extracting the songs and artist tables
        and then write it back to S3
    :param  spark: the Spark Session
    :param  input_data: the location of song_data from where the file is load to process
    :param  output_data: the location where after processing the results will be stored
    :return None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create view for songs table
    df.createOrReplaceTempView("songs")   
    
    
    # extract columns to create songs table. Adding Distinct and Not null to song_id as it is the primary key
    songs_table = spark.sql("""
                            SELECT DISTINCT song_id, 
                            title,
                            artist_id,
                            year,
                            duration
                            FROM songs
                            WHERE song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # create view for artists table
    df.createOrReplaceTempView("artists")   
    
    # extract columns to create artists table, Adding Distinct and Not null to artist_id as it is the primary key
    artists_table = spark.sql("""
                                SELECT DISTINCT artist_id, 
                                artist_name,
                                artist_location,
                                artist_latitude,
                                artist_longitude
                                FROM artists
                                WHERE artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    
    """
    This function loads log_data from S3 and processes it by extracting the user, time and songplays table
        and then write it back to S3
    :param  spark: the Spark Session
    :param  input_data: the location of log_data from where the file is load to process
    :param  output_data: the location where after processing the results will be stored
    :return None
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # create view for users table
    df.createOrReplaceTempView("users")  

    # extract columns for users table, Adding Distinct and Not null to user_id as it is the primary key
    users_table = spark.sql("""
                            SELECT DISTINCT userId as user_id, 
                            firstName as first_name,
                            lastName as last_name,
                            gender as gender,
                            level as level
                            FROM users
                            WHERE userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')  
    
    # create view for time table
    df.createOrReplaceTempView("time") 
    
    # extract columns to create time table, Adding Not null to ts as it is the primary key, its very unlikely that ts will be 
    # same for two rows as its time in miliseconds. Much easier and striaghtforward to extract day, hour, week etc. from timestamp
    # through SQL then converting timestamp in Dataframe to datetime and then performing extraction 
    time_table = spark.sql("""
                            SELECT 
                            timestamp_data.start_time_prev as start_time,
                            hour(timestamp_data.start_time_prev) as hour,
                            dayofmonth(timestamp_data.start_time_prev) as day,
                            weekofyear(timestamp_data.start_time_prev) as week,
                            month(timestamp_data.start_time_prev) as month,
                            year(timestamp_data.start_time_prev) as year,
                            dayofweek(timestamp_data.start_time_prev) as weekday
                            FROM
                            (SELECT to_timestamp(ts/1000) as start_time_prev
                            FROM time
                            WHERE ts IS NOT NULL
                            ) timestamp_data
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/') 
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create view for song data
    df.createOrReplaceTempView("song_data")
    
    # create view for songplays table 
    df.createOrReplaceTempView("songplays") 
    
    
"""
    extract columns from joined song and log datasets to create songplays table, applying monotonically id increasing function 
    to increment id for songplay id and make it unique and not null. Joining the two views songplays and song_data on artist 
    name and song title to obtain the information   # about song_id and artist_id columns. Every song that has a title should 
    have a song id and every artist that has a name should have an aritst id.
"""
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(ts/1000) as start_time,
                                month(to_timestamp(ts/1000)) as month,
                                year(to_timestamp(ts/1000)) as year,
                                userId as user_id,
                                level as level,
                                song_data.song_id,
                                song_data.artist_id,
                                session_id,
                                location,
                                user_agent
                                FROM songplays
                                JOIN song_data on songplays.artist = song_data.artist_name and songplays.song = song_data.title
                            """)


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/ash_rahman/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
