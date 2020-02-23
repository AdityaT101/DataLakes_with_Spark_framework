The Project shows a classic ETL procedure using Spark.

The project comprises of Song_data and Log_data. Both of these data files are hosted on a AWS S3. 

So, we import the data into spark cluster in form of dataframes, then process the data and export it back to S3 again in form of dimensional tables and fact tables.

The 'etl.py' does the actual etl work. It extracts the data from those 2 files - ('song_data' and 'log_data') from S3 , then processes that adata and dumps it in S3 again.


Order of operations:-

We create a spark dataframe to from the data collected from S3 - song_data. Select certain columns from the dataframes to create  tables - Songs and Artist.
Export these parquet files to S3(a different one, though).

Similary Create a spark dataframe to from the data collected from S3 - log_data. Select certain columns from the dataframes to create tables - Users and Time.
Export these parquet files to S3(a different one, though).

Import the 'Song_data' which we generated earlier and perform a JOIN on Log_data which we generated from earlier dataframe. Export this data in form of parquet file back to S3 again.



















