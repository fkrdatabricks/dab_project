from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.remote(cluster_id="0701-160631-7alq42fo").getOrCreate()

spark.sql("select 1").show()