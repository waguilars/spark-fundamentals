import os

from pyspark.sql import SparkSession

os.environ["SPARK_HOME"] = "/opt/spark"

spark = SparkSession.builder.getOrCreate()

# Test Spark
df = spark.createDataFrame([{"hello": "world"} for x in range(1000)])
df.show(3, False)