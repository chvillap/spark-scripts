# Random data generation is useful for testing of existing algorithms and
# implementing randomized algorithms, such as random projection. We provide
# methods under sql.functions for generating columns that contains i.i.d.
# values drawn from a distribution, e.g., uniform (rand), and standard
# normal (randn).

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import rand, randn

spark = SparkSession \
    .builder \
    .appName("Random data generation") \
    .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

# Create a DataFrame with one int column and 10 rows.
df = sqlContext.range(0, 10)
df.show()

# Generate two other columns using uniform distribution and normal
# distribution.
df.select("id",
          rand(seed=10).alias("uniform"),
          randn(seed=27).alias("normal")).show()

spark.stop()
