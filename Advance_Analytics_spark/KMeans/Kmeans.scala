import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._

object Main {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("test")
        val sc = new SparkContext(conf)
        val rawData = sc.textFile("hdfs://10.170.31.120:9000/user/hypnoes/kddcup.data")
        rawData.map(_.split(',').last).countByValue().toSeq.
        sortBy(_._2).reverse.foreach(println)

        val labelsAndData = rawData.map { line =>
            val buffer = line.split(',').toBuffer
            buffer.remove(1, 3)
            val label = buffer.remove(buffer.length-1)
            val vector = Vectors.dense(buffer.map(_.toDouble).toArray)
            (label,vector)
        }
        val data = labelsAndData.values.cache()

        val kmeans = new KMeans()
        val model = kmeans.run(data)
        model.clusterCenters.foreach(println)

        val clusterLabelCount = labelsAndData.map { case (label,datum) =>
            val cluster = model.predict(datum)
            (cluster,label)
        }.countByValue
        clusterLabelCount.toSeq.sorted.foreach {
            case ((cluster,label),count) =>
                println(f"$cluster%1s$label%18s$count%8s")
        }

        def distance(a: Vector, b: Vector) =
            math.sqrt(a.toArray.zip(b.toArray).
            map(p => p._1 - p._2).map(d => d * d).sum)


        def distToCentroid(datum: Vector, model: KMeansModel) = {
            val cluster = model.predict(datum)
            val centroid = model.clusterCenters(cluster)
            distance(centroid, datum)
        }



        def clusteringScore(data: RDD[Vector], k: Int) = {
            val kmeans = new KMeans()
            kmeans.setK(k)
            val model = kmeans.run(data)
            data.map(datum => distToCentroid(datum, model)).mean()
        }

        (5 to 40 by 5).map(k => (k, clusteringScore(data, k))).foreach(println)

    }
}   