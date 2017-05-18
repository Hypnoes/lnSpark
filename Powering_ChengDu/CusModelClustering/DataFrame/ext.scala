import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors

object PoweringChengDu {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()

    val root = "hdfs://10.170.31.120:9000/user/hypnoes/dsq_1M_svm.txt"
    val dataset = spark.read.format("libsvm").load(root)

    val kmm = new KMeans().setK(3)
    val model = kmm.fit(dataset)

    val WSSSE = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    spark.stop()
  }
}

