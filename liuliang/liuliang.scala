import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd
import org.apache.spark.rdd._

object liuliang {

    // The Main Entery.
    def main(args: Array[String]): Unit = {

        val r1 = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r // Find all IP and counting
        val r2 = """(\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)""".r       // Find Only Sender
        val r3 = """(\s>\s)(\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}""".r       // Find Only Receiver
        val ans = "hdfs://10.170.31.120:9000/user/hypnoes/ans"

        val spark = new SparkContext("spark://10.170.31.120:7077", "liuliang")
        val fs = spark.textFile("hdfs://10.170.31.120:9000/user/hypnoes/liuliang.txt")
        args(0) match {
            case "-i" => default(fs, r2).saveAsTextFile(ans + "-in")
            case "-o" => default(fs, r3).saveAsTextFile(ans + "-out")
            case _    => default(fs, r1).saveAsTextFile(ans)
            }
        spark.stop()
    }

    // Default
    def default(fs: RDD[String], r: scala.util.matching.Regex): RDD[(String, Int)] = {
        fs.flatMap(line => r.findAllIn(line)).flatMap(line => line.split(" > ")).map(word => 
            (word, 1)).reduceByKey(_+_)
    }

    // V3: Record the direction.
    def directed(fs: RDD[String]): Unit = {
        val r = """((\d{1,3}\.){3}\d{1,3}\.?[\w\-]{0,}(\s>\s)?){2}""".r
        val ofs = fs.flatMap(line => r.findAllIn(line)).map(word => (word, 1)).reduceByKey(_+_)
        ofs.saveAsTextFile("hdfs://10.170.31.120:9000/user/hypnoes/ans")
    }

    // V4: Sorting?
    def sort(): Unit = {
        // TO DO HERE...
    }
}
