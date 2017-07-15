import org.apache.spark.sql
import org.apache.spark.sql._

object Liuliang {
    def main(args: Array[String]): Unit = {
        val spark= SparkSession
            .builder
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()
        
        val root = "hdfs://10.170.31.120:9000/user/hypnoes"
        val r = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r 
        
        val raw = spark.read.text(root + "liuliang.txt").as[String]
        val df = raw.map(r.findAllIn(_)).map(_.split(" > ")).map(x => (x(0), x(1)))
            .toDF("IN", "OUT").groupBy("IN", "OUT").count.sort($"Count".desc)

        df.write.json(root + "blas")

        spark.stop()
    }
}
