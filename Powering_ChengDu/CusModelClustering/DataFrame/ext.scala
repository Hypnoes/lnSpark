import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.linalg.Vectors

object PoweringChengDu {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()

        val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
        args.foreach(input => {
            train(spark.read.format("libsvm").load(root + "SVM_Data/" +input),
                input)
        })

        spark.stop()
    }

    def train(df: DataFrame, name: String): Unit = {
        val kmm = new KMeans().setK(3)
        val model = kmm.fit(df)

        writeOut(model, name)

        model.transform(df).write
            .json("hdfs://10.170.31.120:9000/user/hypnoes/"
                + "out/" + name.split("svm")(0) + "a")
    }

    def writeOut(model: KMeansModel, name: String): Unit = {
        println("")
        println("=======================" + name + "===========================")
        println("Cluster Centers: ")
        model.clusterCenters.foreach(println)
        println("=======================" + name + "===========================")
        println("")
    }
}

