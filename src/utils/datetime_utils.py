from pyspark.sql.functions import current_timestamp, col, to_date 

def timestamp_to_date_col(spark, df, timestamp_col, output_col):
    """
    Extracts the date from a timestamp column and adds it as a new column in the dataframe.
 
    Parameters:
      spark: Spark Session
      df (Dataframe): Input Pyspark dataframe containing the timestamp.
      timestamp_col (str): The name of the column containing the timestamp.
      output_col (str): The name for the output column with the ride date.

    Retruns:
      Dataframe: A new dataframe with the additional ride date column
    """
    # Use to_date to extract the date part of the timestamp
    return df.withColumn(output_col, to_date(col(timestamp_col))    )

