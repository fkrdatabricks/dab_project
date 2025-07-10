import sys
import os
import pytest 

sys.path.append(os.getcwd())

@pytest.fixture()
def spark():
    # Create a spark session
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
        print("Remote spark session created....")
    except ImportError:
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            print("Local spark session created....")
        except:
            raise ImportError("Neither Databricks Session or Spark Session are available")
    return spark