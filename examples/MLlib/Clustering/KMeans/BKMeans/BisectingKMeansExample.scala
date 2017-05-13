// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.SparkSession
// $example off$

/**
 * An example demonstrating bisecting k-means clustering.
 * Run with
 * {{{
 * bin/run-example ml.BisectingKMeansExample
 * }}}
 */
object BisectingKMeansExample {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession
    val spark = SparkSession
      .builder
      .appName("BisectingKMeansExample")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm").load("data/mllib/sample_kmeans_data.txt")

    // Trains a bisecting k-means model.
    val bkm = new BisectingKMeans().setK(2).setSeed(1)
    val model = bkm.fit(dataset)

    // Evaluate clustering.
    val cost = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $cost")

    // Shows the result.
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
