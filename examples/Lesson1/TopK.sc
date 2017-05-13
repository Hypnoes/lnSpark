import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TopK {
    def main(args:Array[String]) {
        val spark = new SparkContext("local", "TopK",
            System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
        val count = spark.textFile("data").flatMap(line =>
            line.split(" ")).map(word =>
            (word, 1)).reduceByKey(_+_)
        val topk = count.mapPartitions(iter => {
            while(iter.hasNext) {
                putToHeap(iter.next())
            }
            getHeap().iterator
            }).collect()
        val iter = topk.iterator
        while(iter.hasNext) {
            putToHeap(iter.next())
        }
        val outiter = getHeap().iterator
        println("TopK 值: ")
        while(outiter.hasNext) {
            println("\n 词频: " + outiter.next()._1 + " 词: " + outiter.next._2)
        }
        spark.stop()
    }
    def putToHeap(iter : (String, Int)) {
        /* ... */
    }

    def getHeap(): Array[(String, Int)] {
        /* ... */
        val a = new Array[(String, Int)]()
    }
}
