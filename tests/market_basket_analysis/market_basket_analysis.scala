import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.HashMap

// val conf = new SparkConf()
//     .setAppName("MBA")
//     .setMaster("local[*]")
//     .set("spark.storage.memoryFraction", "1")

// val sc = new SparkContext(conf)

// Set parameters.
val minSupport = 0.25
val minConfidence = 0.8
val numPartitions = 5

// Load transaction source.
val data = sc.textFile("dummytrans.txt")

// Map operation split by line.
val transactions = data.map(x => x.split(" "))
transactions.cache()

// ...
val totalNumTransactions: Double = transactions.count()

// Create a FPGrowth instance.
val fpg = new FPGrowth()
fpg.setMinSupport(minSupport)
fpg.setNumPartitions(numPartitions)

// Compute the FPGrowth model.
val model = fpg.run(transactions)

// ...
val supports = new HashMap[String, Double]()

// Print out the frequent item sets.
println("===== FREQUENT ITEM SETS =====")
model.freqItemsets.collect().foreach(itemset => {
    val itemsetName = itemset.items.mkString("[", ",", "]")

    supports(itemsetName) = itemset.freq / totalNumTransactions
    println(itemsetName + ", support = " + itemset.freq / totalNumTransactions)
})

// Print out the association rules.
println("===== ASSOCIATION RULES =====")
// Recommendation rules generation:
// Antecedent represents X:   X -> Y
// Consequent represents Y:   X -> Y
// Confidence represents confidence.
// Of course, the result can be stored in DB or sent to marketing for
// further activities.
// Also could be used for near real time recommendation by other
// APIs/in-memory DB, etc.
val rules = model.generateAssociationRules(minConfidence)
rules.collect().foreach(rule => {
    val antecedentName = rule.antecedent.mkString("[", ",", "]")
    val consequentName = rule.consequent.mkString("[", ",", "]")
    val lift = rule.confidence / supports(consequentName)

    println(antecedentName + " => " + consequentName +
        ", confidence = " + rule.confidence +
        ", lift = " + lift)
})

// Print how many rules have been generated.
val totalNumRules: Double = rules.collect().length
println("\nTotal number of rules: " + totalNumRules)

// Terminate the program.
sc.stop()
System.exit(0)
