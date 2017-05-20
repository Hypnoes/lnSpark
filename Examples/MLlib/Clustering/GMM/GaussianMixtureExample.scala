// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql.SparkSession
// $example off$

/**
 * An example demonstrating Gaussian Mixture Model (GMM).
 * Run with
 * {{{
 * bin/run-example ml.GaussianMixtureExample
 * }}}
 */

object GaussianMixtureExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()

    // $example on$
    // Loads data
    val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
    val dataset = spark.read.format("libsvm").load(root + "sample_kmeans_data.txt")

    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(2)
    val model = gmm.fit(dataset)

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
          s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
