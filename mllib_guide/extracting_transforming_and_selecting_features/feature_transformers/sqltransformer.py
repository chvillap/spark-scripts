from pyspark.sql import SparkSession
from pyspark.ml.feature import SQLTransformer

# SQLTransformer implements the transformations which are defined by SQL
# statement. Currently we only support SQL syntax like
# "SELECT ... FROM __THIS__ ..." where "__THIS__" represents the underlying
# table of the input dataset. The select clause specifies the fields,
# constants, and expressions to display in the output, and can be any select
# clause that Spark SQL supports. Users can also use Spark SQL built-in
# function and UDFs to operate on these selected columns. For example,
# SQLTransformer supports statements like:
#    SELECT a, a + b AS a_b FROM __THIS__
#    SELECT a, SQRT(b) AS b_sqrt FROM __THIS__ where a > 5
#    SELECT a, b, SUM(c) AS c_sum FROM __THIS__ GROUP BY a, b

spark = SparkSession.builder.appName("SQLTransformer").getOrCreate()

df = spark.createDataFrame([
    (0, 1.0, 3.0),
    (2, 2.0, 5.0)
], ["id", "v1", "v2"])

sqlTrans = SQLTransformer(
    statement="SELECT *, (v1+v2) AS v3, (v1*v2) AS v4 FROM __THIS__")

sqlTrans.transform(df).show()

spark.stop()
