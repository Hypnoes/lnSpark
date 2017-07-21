import org.apache.spark.sql.SparkSession

object CusEpPlA {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()
        import spark.implicits._

        val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
        
        // To Do Here...
        
        spark.stop()
    }
}

