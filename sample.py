from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
df=spark.createDataFrame(data, schema=columns)
df.show()
