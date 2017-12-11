from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Load/Save 1").setMaster("local")
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName("Load/Save 1").getOrCreate()

# In the simplest form, the default data source (parquet unless otherwise
# configured by spark.sql.sources.default) will be used for all operations.

df1 = ss.read.load("users.parquet")
df1.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

# You can also manually specify the data source that will be used along with
# any extra options that you would like to pass to the data source. Data
# sources are specified by their fully qualified name (i.e.,
# org.apache.spark.sql.parquet), but for built-in sources you can also use
# their short names (json, parquet, jdbc, orc, libsvm, csv, text). DataFrames
# loaded from any data source type can be converted into other types using
# this syntax.

df2 = ss.read.load("people.json", format="json")
df2.select("name", "age").write.save("namesAndAges.parquet", format="parquet")

print("OK!")

ss.stop()
sc.stop()
