import org.apache.spark.SparkContexxt
import scala.collection.mutable._

object InvertedIndex {
    def main(args : Attay[String]) {
        val spark = new SparkContext("local", "TopK",
            System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
        val words = spark.textFile("dir").map(file => file.split("\t")).map(item => {
            (item(0), item(1))
        }).flatMap(file => {
            val list = new LinkedList[(String, String)]
            val words = file._2.split(" ").iterator
            while(words.hasNext) {
                list+words.next()
            }
            list
        }).distinct()
        words.map(word => {
            (word._2, word._1)
        }).reduce((a, b) => {
            a+"\t"+b
        }).saveAsTextFile("index")
    }
}