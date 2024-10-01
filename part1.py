from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
data = [("John", 30), ("Doe", 25), ("Jane", 28)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()