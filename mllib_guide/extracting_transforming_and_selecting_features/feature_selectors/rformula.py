from pyspark.sql import SparkSession
from pyspark.ml.feature import RFormula

# RFormula selects columns specified by an R model formula. Currently we
# support a limited subset of the R operators, including '~', '.', ':', '+',
# and '-'. The basic operators are:
#    ~ separate target and terms
#    + concat terms, "+ 0" means removing intercept
#    - remove a term, "- 1" means removing intercept
#    : interaction (multiplication for numeric values, or binarized
#      categorical values)
#    . all columns except target

# RFormula produces a vector column of features and a double or string column
# of label. Like when formulas are used in R for linear regression, string
# input columns will be one-hot encoded, and numeric columns will be cast to
# doubles. If the label column is of type string, it will be first transformed
# to double with StringIndexer. If the label column does not exist in the
# DataFrame, the output label column will be created from the specified
# response variable in the formula.

spark = SparkSession.builder.appName("RFormula").getOrCreate()

dataset = spark.createDataFrame(
    [(7, "US", 18, 1.0),
     (8, "CA", 12, 0.0),
     (9, "NZ", 15, 0.0)],
    ["id", "country", "hour", "clicked"])

formula = RFormula(
    formula="clicked ~ country + hour",
    featuresCol="features",
    labelCol="label")

model = formula.fit(dataset)

output = model.transform(dataset)
output.select("features", "label").show()

spark.stop()
