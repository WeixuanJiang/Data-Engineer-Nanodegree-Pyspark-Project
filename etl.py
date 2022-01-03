import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType,DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a Apache Spark session to process the data.
    Keyword arguments:
    * N/A
    Output:
    * spark -- An Apache Spark session.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Load JSON input data (song_data) from input_data path,
        process the data to extract song_table and artists_table, and
        store the queried data to parquet files.
    Keyword arguments:
    * spark         -- reference to Spark session.
    * input_data    -- path to input_data to be processed (song_data)
    * output_data   -- path to location to store the output (parquet files).
    Output:
    * songs_table   -- directory with parquet files
                       stored in output_data path.
    * artists_table -- directory with parquet files
                       stored in output_data path.
    """
        
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView('song')
    
    songs_table = spark.sql("""
        SELECT 
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM song
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data + "songs_table")

    # extract columns to create artists table
    df.createOrReplaceTempView('artist')
    artists_table = spark.sql("""
        SELECT 
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM artist
                                """)

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table")


def process_log_data(spark, input_data, output_data):
    
    """Load JSON input data (log_data) from input_data path,
        process the data to extract users_table, time_table,
        songplays_table, and store the queried data to parquet files.
    Keyword arguments:
    * spark            -- reference to Spark session.
    * input_data       -- path to input_data to be processed (log_data)
    * output_data      -- path to location to store the output
                          (parquet files).
    Output:
    * artists_table      -- directory with  artists_table parquet files
                          stored in output_data path.
    * time_table       -- directory with time_table parquet files
                          stored in output_data path.
    * songplayes_table -- directory with songplays_table parquet files
                          stored in output_data path.
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page=='NextSong')
    df = df.dropDuplicates()

    # extract columns for users table    
    artists_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    
    # write users table to parquet files
    artists_table.write.parquet(output_data + "users_table")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0),TimestampType())
    df = df.withColumn('start_time',get_timestamp('ts'))
    df.createOrReplaceTempView("log_data")
    
   # create other time columns
    df = df.withColumn('year',hour('start_time'))
    df = df.withColumn('month',month('start_time'))
    df = df.withColumn('day',dayofmonth('start_time'))
    df = df.withColumn('hour',hour('start_time'))
    df = df.withColumn('week',weekofyear('start_time'))
    df = df.withColumn('weekday',date_format("start_time",'MM/dd/yyy'))
    
    # extract columns to create time table
    time_table = df.select('start_time','year','month','week','day','hour','weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(output_data + "time_table")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/")

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songs_table")
    artist_df = spark.read.parquet(output_data + "artists_table")
    artist_df.createOrReplaceTempView("artists_table")
    time_table.createOrReplaceTempView("time_table")
    
    songplays_table = spark.sql("""
                                SELECT log_data.start_time,
                                       time_table.year,
                                       time_table.month,
                                       q.song_id,
                                       q.artist_id,
                                       log_data.userid,
                                       log_data.level,
                                       log_data.sessionid,
                                       log_data.location,
                                       log_data.useragent
                                  FROM log_data
                                  JOIN time_table ON (log_data.start_time = time_table.start_time)
                                  LEFT JOIN (
                                             SELECT songs_table.song_id,
                                                  songs_table.title,
                                                  artists_table.artist_id,
                                                  artists_table.artist_name
                                             FROM songs_table 
                                             JOIN artists_table ON (songs_table.artist_id = artists_table.artist_id)
                                      )      AS q ON (log_data.song = q.title AND log_data.artist = q.artist_name)
                               """)
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(output_data + "songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udProject4PySparkLJ/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
