# import sys
# import os
# import pytest 

# sys.path.append(os.getcwd())

import datetime
from src.utils.datetime_utils import timestamp_to_date_col
# from pyspark.sql import SparkSession

def test_timestamp_to_date_col(spark):
    # # Create a spark session
    # try:
    #     from databricks.connect import DatabricksSession
    #     spark = DatabricksSession.builder.getOrCreate()
    # except ImportError:
    #     try:
    #         from pyspark.sql import SparkSession
    #         spark = SparkSession.builder.getOrCreate()
    #     except:
    #         raise ImportError("Neither Databricks Session or Spark Session are available")

    # Create a test dataframe with known start and end timestamps using datetime objects
    data = [(
        datetime.datetime(2025, 4, 10, 10,30,0),)
      ]

    schema = "ride_timestamp timestamp"
    df = spark.createDataFrame(data, schema = schema)

    # Use the utility to add a date column
    result_df = timestamp_to_date_col(spark, df, "ride_timestamp", "ride_date")

    # Assert that the extracted date matches the expected value
    row = result_df.select("ride_date").first()
  
    expected_date = datetime.date(2025, 4, 10) #Expected: 2025-04-10

    assert row["ride_date"] == expected_date
