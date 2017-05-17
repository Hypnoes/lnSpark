import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession


object PoweringChengDu {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()

    val root = "hdfs://10.170.31.120:9000/user/hypnoes/out.txt"
    val dataset = spark.read.format("libsvm").load(root);

    val ks = Array(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
    ks.foreach(k => {
      val kmm = new KMeans().setK(k)
      val model = kmm.fit(dataset)

      val WSSSE = model.computeCost(dataset)
      println(s"While K = $k, Sum of Squared Errors = $WSSSE")
    })

    //println("Cluster Centers: ")
    //model.clusterCenters.foreach(println)

    spark.stop()
  }
}

