from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans

# K-means is one of the most commonly used clustering algorithms that clusters
# the data points into a predefined number of clusters. The spark.ml
# implementation includes a parallelized variant of the k-means++ method called
# kmeans||. The implementation in spark.ml has the following parameters:
#   - k: the number of desired clusters. Note that it is possible for fewer
#     than k clusters to be returned, for example, if there are fewer than k
#     distinct points to cluster.
#   - maxIterations: the maximum number of iterations to run.
#   - initializationMode: specifies either random initialization or
#     initialization via k-means||.
#   - runs: This param has no effect since Spark 2.0.0.
#   - initializationSteps: determines the number of steps in the k-means||
#     algorithm.
#   - epsilon: determines the distance threshold within which we consider
#     k-means to have converged.
#   - initialModel: an optional set of cluster centers used for initialization.
#     If this parameter is supplied, only one run is performed.

spark = SparkSession.builder.appName("KMeans").getOrCreate()

# Load data.
dataset = spark.read.format("libsvm") \
                    .load("sample_kmeans_data.txt") \
                    .toDF("id", "features")

# Train a K-means model.
# kmeans = KMeans(k=2, seed=1)
kmeans = KMeans().setK(2).setSeed(1)
model = kmeans.fit(dataset)

# Evaluate clustering by computing Within Set Sum of Squared Errors.
wssse = model.computeCost(dataset)
print("Within Set Sum of Squared Errors = " + str(wssse))

# Show the result.
centers = model.clusterCenters()
print("Cluster centers:")
for center in centers:
    print(center)

clustered = model.transform(dataset)
clustered.show()

spark.stop()
