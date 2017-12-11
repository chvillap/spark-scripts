from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName("Dataframe Operations").setMaster("local")
sc = SparkContext(conf=conf)

ss = SparkSession.builder.appName("Dataframe Operations").getOrCreate()

# DataFrames provide a domain-specific language for structured data
# manipulation in Scala, Java, Python and R.
# In Spark 2.0, DataFrames are just Dataset of Rows in Scala and Java API.
# These operations are also referred as "untyped transformations" in contrast
# to "typed transformations" come with strongly typed Scala/Java Datasets.

df = ss.read.json("people.json")

# Print the schema in a tree format.
df.printSchema()

# Select only the "name" column.
df.select("name").show()

# Select the "name" and "age" columns.
df.select("name", "age").show()

# Select everybody, but increment the age by 1.
df.select(df["name"], df["age"] + 1).show()

# Select people older than 21.
df.filter(df["age"] > 21).show()

# Count people by age.
df.groupBy("age").count().show()


# In addition to simple column references and expressions, DataFrames also have
# a rich library of functions including string manipulation, date arithmetic,
# common math operations and more.

ss.stop()
sc.stop()
