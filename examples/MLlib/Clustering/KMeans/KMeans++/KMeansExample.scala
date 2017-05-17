// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession
// $example off$

/**
 * An example demonstrating k-means clustering.
 * Run with
 * {{{
 * bin/run-example ml.KMeansExample
 * }}}
 */

object KMeansExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    // $example on$
    // Loads data.
    val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
    val dataset = spark.read.format("libsvm").load(root + "data/mllib/sample_kmeans_data.txt")

    // Trains a k-means model.
    val kmeans = new KMeans().setK(2).setSeed(1L)
    val model = kmeans.fit(dataset)

    // Evaluate clustering by computing Within Set Sum of Squared Errors.
    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Shows the result.
    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
