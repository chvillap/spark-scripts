# The function describe returns a DataFrame containing information such as
# number of non-null entries (count), mean, standard deviation, and minimum
# and maximum value for each numerical column.

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import rand, randn
from pyspark.sql.functions import mean, min, max

spark = SparkSession \
    .builder \
    .appName("Summary and descriptive statistics") \
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

# A slightly different way to generate the two random columns.
df = sqlContext.range(0, 10) \
               .withColumn("uniform", rand(seed=10)) \
               .withColumn("normal", randn(seed=27))

df.describe().show()

# If you have a DataFrame with a large number of columns, you can also run
# describe on a subset of the columns:
df.describe("uniform", "normal").show()

# Of course, while describe works well for quick exploratory data analysis,
# you can also control the list of descriptive statistics and the columns
# they apply to using the normal select on a DataFrame:
df.select([mean("uniform"), min("uniform"), max("uniform")]).show()

spark.stop()
