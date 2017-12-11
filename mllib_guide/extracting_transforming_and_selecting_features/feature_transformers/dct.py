from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import DCT

# The Discrete Cosine Transform transforms a length N real-valued sequence in
# the time domain into another length N real-valued sequence in the frequency
# domain. A DCT class provides this functionality, implementing the DCT-II and
# scaling the result by sqrt(2) such that the representing matrix for the
# transform is unitary. No shift is applied to the transformed sequence (e.g.
# the 0th element of the transformed sequence is the 0th DCT coefficient and
# not the N/2th).

# PS: The obvious distinction between a DCT and a DFT is that the former uses
# only cosine functions, while the latter uses both cosines and sines (in the
# form of complex exponentials).

spark = SparkSession.builder.appName("DCT").getOrCreate()

df = spark.createDataFrame([
    (Vectors.dense([0.0, 1.0, -2.0, 3.0]),),
    (Vectors.dense([-1.0, 2.0, 4.0, -7.0]),),
    (Vectors.dense([14.0, -2.0, -5.0, 1.0]),)
], ["features"])

dct = DCT(inverse=False, inputCol="features", outputCol="featuresDCT")

dctDF = dct.transform(df)
dctDF.select("featuresDCT").show(truncate=False)
