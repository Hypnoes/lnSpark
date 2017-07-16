import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.Vectors

object CusModelCluster {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()
        import spark.implicits._

        val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
        args.foreach(input => {
            val df = spark.read.format("libsvm").load(root + "SVM_Data/" +input)
            val kmm = new KMeans().setK(3)
            val model = kmm.fit(df)
            val ce = model.clusterCenters.toSeq.map(x => col(x)).toDS
            
            model.transform(ce).write
                .json(root+ "out/" + input.split("svm")(0) + "c")
            model.transform(df).write
                .json(root+ "out/" + input.split("svm")(0) + "a")
        })
        
        spark.stop()
    }
        
    case class col(features: org.apache.spark.ml.linalg.Vector)
}

