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

        val spark = new SparkContext("spark://10.170.31.120:7077", "liuliang")
        val fs = spark.textFile("hdfs://10.170.31.120:9000/user/hypnoes/liuliang.txt")
        val meth = default(fs)(_, _)
        args(0) match {
            case "-o" => meth(r3, 3)
            case "-i" => meth(r2, 2)
            case "-d" => meth(r1, 0)
            case _    => meth(r1, 1)
            }
        spark.stop()
    }

    // Default Method.
    def default(fs: RDD[String])(r: scala.util.matching.Regex, mod: Int = 0): Unit = {
        fs.flatMap(line => r.findAllIn(line)).flatMap(line => if(mod > 0) line else line.split(" > ")).map(word =>
            (word, 1)).reduceByKey(_+_).sortBy(x => x._2, false).saveAsTextFile("hdfs://10.170.31.120:9000/user/hypnoes/ans")
    }
}
