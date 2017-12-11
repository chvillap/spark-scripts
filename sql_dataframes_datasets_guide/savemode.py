from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Save Mode").setMaster("local")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName("Save Mode").getOrCreate()

# Save operations can optionally take a SaveMode, that specifies how to handle
# existing data if present. It is important to realize that these save modes
# do not utilize any locking and are not atomic. Additionally, when performing
# an Overwrite, the data will be deleted before writing out the new data.

df = ss.read.load("users.parquet").select("name", "favorite_numbers")
df.write.save("nameAndFavNumbers.json", mode="error", format="json")
df.write.save("nameAndFavNumbers.json", mode="overwrite", format="json")
df.write.save("nameAndFavNumbers.json", mode="append", format="json")
df.write.save("nameAndFavNumbers.json", mode="ignore", format="json")

ss.stop()
sc.stop()
