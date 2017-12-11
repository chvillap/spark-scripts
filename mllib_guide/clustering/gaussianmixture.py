from pyspark.sql import SparkSession
from pyspark.ml.clustering import GaussianMixture

# A Gaussian Mixture Model represents a composite distribution whereby points
# are drawn from one of k Gaussian sub-distributions, each with its own
# probability. The spark.ml implementation uses the expectation-maximization
# algorithm to induce the maximum-likelihood model given a set of samples. The
# implementation has the following parameters:
#   - k is the number of desired clusters.
#   - convergenceTol is the maximum change in log-likelihood at which we
#     consider convergence achieved.
#   - maxIterations is the maximum number of iterations to perform without
#     reaching convergence.
#   - initialModel is an optional starting point from which to start the EM
#     algorithm. If this parameter is omitted, a random starting point will be
#     constructed from the data.

spark = SparkSession.builder.appName("GaussianMixture").getOrCreate()

# Load data.
dataset = spark.read.format("libsvm") \
                    .load("sample_kmeans_data.txt") \
                    .toDF("id", "features")

gmm = GaussianMixture().setK(2).setSeed(1)
model = gmm.fit(dataset)

print("Gaussians shown as a DataFrame:")
model.gaussiansDF.show(truncate=False)

clustered = model.transform(dataset)
clustered.show()

spark.stop()
