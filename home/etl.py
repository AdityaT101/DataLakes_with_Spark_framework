import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime


#Using the Key_id and access key and storing them into local variables.   
os.environ['AWS_ACCESS_KEY_ID']='AKIATILXASXETUxxxxx'
os.environ['AWS_SECRET_ACCESS_KEY']='k8yElAFcsnXTV+uFZTSD5cMAhcZ+Kuratxxxxxx'


#Creating a spark session. 
def create_spark_session():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


#=============================

def build_song_schema():
    """Build and return a schema to use for the song data.
    Returns schema: StructType object, a representation of schema and defined fields
    """
    schema = T.StructType(
        [
            T.StructField('artist_id', T.StringType(), True),
            T.StructField('artist_latitude', T.DecimalType(), True),
            T.StructField('artist_longitude', T.DecimalType(), True),
            T.StructField('artist_location', T.StringType(), True),
            T.StructField('artist_name', T.StringType(), True),
            T.StructField('duration', T.DecimalType(), True),
            T.StructField('num_songs', T.IntegerType(), True),
            T.StructField('song_id', T.StringType(), True),
            T.StructField('title', T.StringType(), True),
            T.StructField('year', T.IntegerType(), True)
        ]
    )
    return schema



'''
In 'process_song_data' function we import song_data from S3, then we Select certain columns from the dataframes to create Songs and Artists tables.
We further export these tables to parquet files in S3.
'''
def process_song_data(spark):
    #get filepath to song data file.
    #please note that I have taken only a part of dataset to process the data faster. 
    #processing all the data on Spark was taking a lot of time.
    song_data = 's3a://udacity-dend/song_data/A/A/*/*'
    
    songSchema = build_song_schema()
    
    #read song data file based on the pre-defined schema
    df = spark.read.json(song_data, schema=songSchema )
     
        
    #extract columns to create songs table
    songs_table = df.select([ 'song_id' , 'title', 'artist_id', 'year', 'duration' ])
    
    
    #write songs table to parquet files partitioned by year and artist
    songs_table = songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet('s3a://sondatabucket/'+'songs_1')

    
    # extract columns to create artists table
    artists_table = df.select([ 'artist_id' , 'artist_name' , 'artist_location' , 'artist_latitude' , 'artist_longitude']) 
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    
    # write artists table to parquet files
    artists_table = artists_table.write.mode('overwrite').parquet('s3a://sondatabucket/'+'artists_1')
     
  



#=============================



'''
In 'process_log_data' function we import log_data from S3, then we Select certain columns from the dataframes to create User table.
We further create TimeTable based on extracting values based on 'ts' column.
We then perform a join between the song data and log data to create a songplays table.
We further export this table into S3.
'''
 
      
def process_log_data(spark):
     
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log_data/*/*/*'

    logSchema = build_log_schema()
            
    # read log data file
    df = spark.read.json( log_data)
    
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    #have a new column for userId, having datatype as integer
    df = df.withColumn( "userId_new", df["userId"].cast( T.IntegerType() ) )
    
    
    #extract columns for users table    
    users_table = df.select([ 'userId' , 'firstName' , 'lastName' , 'gender' , 'level' ])
   
    
    #write users table to parquet files
    users_table = users_table.write.mode('overwrite').parquet('s3a://sondatabucket/'+'users_1')

    
    #create timestamp column from original timestamp columns
    get_timestamp = udf( lambda x: ( x/1000 ) ) 
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    
    #create datetime column from original timestamp column
    #Also, extract year, month, hour, dat, week etc from the timestamp.
    get_datetime = F.udf( lambda x: datetime.fromtimestamp( (x) ), T.TimestampType() )
    df = df.withColumn( "datetime", get_datetime("timestamp") ) 
            
        
    df = df.withColumn( "datetime1", get_datetime("timestamp").cast( T.LongType() ) ) 
    df = df.withColumn( "year" ,  year('datetime') )
    df = df.withColumn( "month" ,  month('datetime') )
    df = df.withColumn( "week" ,  weekofyear('datetime') )
    df = df.withColumn( "day" ,  dayofmonth('datetime') )
    df = df.withColumn( "hour" ,  hour('datetime') )
    
    df.persist()
    
    
     
    #extract columns to create time table
    time_table =  df.select([ "datetime1" , "datetime", "year" ,"month" ,"week" ,"day" ,"hour" ])
     
    
    #write time table to parquet files partitioned by year and month
    time_table = time_table.write.mode('overwrite').partitionBy("year", "month").parquet('s3a://sondatabucket/'+'time_1')
     
        
     
    #read in song data to use for songplays table
    song_df = spark.read.parquet('s3a://sondatabucket/songs_1/')
    song_df.createOrReplaceTempView("songsView")
    song_df.persist()
      
     
      
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = ( 
        df.withColumn( "songplay_id", F.monotonically_increasing_id() ).join( song_df, song_df.title == df.song ).
        select( 
                  "songplay_id", 
                  df.datetime1,
                  col("userId_new").alias("user_id_new"),
                  "level", 
                  "song_id", 
                  "artist_id", 
                  col("sessionId").alias("session_id"),
                  "location", 
                  col("userAgent").alias("user_agent"),            
                  df.year,
                  df.month
                  
              )
        )
    
      
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet('s3a://sondatabucket/'+'songplays_1')
     
     
    

    
    
#=======================================================================
  
    
    

def main():
    spark = create_spark_session()
    
    #increase the timeout from 300 ms to 36000 ms
    spark.conf.set("spark.sql.broadcastTimeout",  36000)
        
    #calling the function to process the song_data
    process_song_data(spark) 
    
    
    #calling the function to process the log_data
    process_log_data(spark)

    
    ''' # This is to test the songplays_1 data which we exported to S3 earlier. We can load it using parquet files and then view the data.
    songplays_df = spark.read.parquet('s3a://sondatabucket/songplays_1/')
    songplays_df.createOrReplaceTempView("songplaysView")
    songplays_df.show(2)
    ''' 

    
if __name__ == "__main__":
    main()

    
       