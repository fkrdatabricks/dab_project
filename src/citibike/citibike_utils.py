from pyspark.sql.functions import col, unix_timestamp

def get_trip_duration_mins(spark, df, start_col, end_col, output_col):
    """
    Adds a column to the dataframe calculating the difference in minutes between two timestamp columns.

    parameters:
       spark: Spark Session
       df: Spark Dataframe
       start_col (str): Name of the column with the start timestamp
       end_col (str): Name of the column with the end timestamp
       output_col (str): Name of the resulting column

       Returns:
         Dataframe with an additional column showing the difference in minutes.
    """
    return df.withColumn(output_col, (unix_timestamp(col(end_col) ) - unix_timestamp(col(start_col))) /60 )


