import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object liuliang {

    // The Main Entery.
    def main(args: Array[String]): Unit = {
        default()
    }
    // Default: Find all and counting.
    def default(): Unit = {
        val r = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r
        val spark = new SparkContext("spark://Master:7077", "liuliang")
        val fs = spark.textFile("hdfs://Master:9000/firewall/liuliang.txt").flatMap(line => 
            r.findAllIn(line)).flatMap(line => line.split(" > ")).map(word => 
            (word, 1)).reduceByKey(_+_)
        fs.saveAsTextFile("hdfs://Master:9000/firewall/ans-d")
        spark.stop()
    }
    
    // V1: Find Only Sender.
    def v1(): Unit = {
        val r = """(\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)""".r
        val spark = new SparkContext("spark://Master:7077", "liuliang")
        val fs = spark.textFile("hdfs://Master:9000/firewall/liuliang.txt").flatMap(line => 
            r.findAllIn(line)).flatMap(line => line.split(" > ")).map(word => 
            (word, 1)).reduceByKey(_+_)
        fs.saveAsTextFile("hdfs://Master:9000/firewall/ans-in")
        spark.stop()
    }
    
    // V2: Find Only Receiver.
    def v2(): Unit = {
        val r = """(\s>\s)(\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}""".r
        val spark = new SparkContext("spark://Master:7077", "liuliang")
        val fs = spark.textFile("hdfs://Master:9000/firewall/liuliang.txt").flatMap(line => 
            r.findAllIn(line)).flatMap(line => line.split(" > ")).map(word => 
            (word, 1)).reduceByKey(_+_)
        fs.saveAsTextFile("hdfs://Master:9000/firewall/ans-out")
        spark.stop()
    }

    // V3: Record the direction.
    def v3(): Unit = {
        val r = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r
        val spark = new SparkContext("spark://Master:7077", "liuliang")
        val fs = spark.textFile("hdfs://Master:9000/firewall/liuliang.txt").flatMap(line => 
            r.findAllIn(line)).map(word => (word, 1)).reduceByKey(_+_)
        fs.saveAsTextFile("hdfs://Master:9000/firewall/ans")
        spark.stop()
    }

    // V4: Sorting?
    def v4(): Unit = {
        // TO DO HERE...
    }
}
