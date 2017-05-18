import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://10.170.31.120:9000/user/hypnoes/out.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    val clusters = new KMeans()
    val model = clusters.setK(3).run(parsedData)

    println("Clust Centers: ")
    model.clusterCenters.foreach(println)

    println("Predict: ")
    model.predict(parsedData).collect.foreach(println)

    val WSSSE = model.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    sc.stop()
  }
}
