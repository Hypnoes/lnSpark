import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object NumOnce {
    def computeoneNum(args: Array[String]) {
        val spark = new SparkContext("local[1]", "NumOnce",
            System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
        val data = spark.textFile("data")
        val result = data.mapPartitions(iter => {
            var temp = iter.next()
            while(iter.hasNext) {
                temp = temp^iter.next()
            }
            Seq((1,temp)).iterator
        }).reduceByKey(_^_).collect()
        println("num appear once is :"+result(0))
    }
} 