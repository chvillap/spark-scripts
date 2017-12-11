from pyspark.sql import SparkSession
from pyspark.ml.clustering import LDA

# Latent Dirichlet allocation (LDA) is a topic model which infers topics from
# a collection of text documents. LDA can be thought of as a clustering
# algorithm as follows:
#   - Topics correspond to cluster centers, and documents correspond to
#     examples (rows) in a dataset.
#   - Topics and documents both exist in a feature space, where feature vectors
#     are vectors of word counts (bag of words).
#   - Rather than estimating a clustering using a traditional distance, LDA
#     uses a function based on a statistical model of how text documents are
#     generated.

# LDA supports different inference algorithms via setOptimizer function.
# EMLDAOptimizer learns clustering using expectation-maximization on the
# likelihood function and yields comprehensive results, while
# OnlineLDAOptimizer uses iterative mini-batch sampling for online variational
# inference and is generally memory friendly.

# LDA takes in a collection of documents as vectors of word counts and the
# following parameters (set using the builder pattern):
#   - k: Number of topics (i.e., cluster centers)
#   - optimizer: Optimizer to use for learning the LDA model, either
#     EMLDAOptimizer or OnlineLDAOptimizer
#   - docConcentration: Dirichlet parameter for prior over documents'
#     distributions over topics. Larger values encourage smoother inferred
#     distributions.
#   - topicConcentration: Dirichlet parameter for prior over topics'
#     distributions over terms (words). Larger values encourage smoother
#     inferred distributions.
#   - maxIterations: Limit on the number of iterations.
#   - checkpointInterval: If using checkpointing (set in the Spark
#     configuration), this parameter specifies the frequency with which
#     checkpoints will be created. If maxIterations is large, using
#     checkpointing can help reduce shuffle file sizes on disk and help with
#     failure recovery.

# All of spark.mllib’s LDA models support:
#   - describeTopics: Returns topics as arrays of most important terms and term
#     weights.
#   - topicsMatrix: Returns a vocabSize by k matrix where each column is a
#     topic.

spark = SparkSession.builder.appName("LDA").getOrCreate()

# Load data.
dataset = spark.read.format("libsvm") \
                    .load("sample_lda_libsvm_data.txt") \
                    .toDF("id", "features")
dataset.show(truncate=False)

# Train a LDA model.
lda = LDA(k=10, maxIter=10)
model = lda.fit(dataset)

ll = model.logLikelihood(dataset)
lp = model.logPerplexity(dataset)

print("The lower bound on the log likelihood of the entire corpus: " + str(ll))
print("The upper bound bound on perplexity: " + str(lp))

# Describe topics.
topics = model.describeTopics(3)
print("The topics described by their top-weighted terms:")
topics.show(truncate=False)

# Show the result.
transformed = model.transform(dataset)
transformed.show(truncate=False)

spark.stop()
