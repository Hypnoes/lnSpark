import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext.rddToPairRDDFunctions

object Median {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val data = spark.textFile("data")
    val mappeddata = data.map(num => {
        (num/1000, num)
    })
    val count = mappeddata.reduceByKey((a,b) => {
        a+b
    }).collect()
    val sum_count = count.map(data => {
        data._2
    }).sum
    var temp = 0
    var index = 0
    var mid = sum_count/2
    for(i <- 0 to 10) {
        temp = temp + count(i)
        if (temp >= mid) {
            index = i
            break
        }
    }
}