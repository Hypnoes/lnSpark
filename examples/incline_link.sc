import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SkewJoin {
    def main(args : Array[String]) {
        val skewedTable = left.execute()
        val spark = new SparkContext("local", "TopK",
            System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
        val skewTable = spark.textFile("skewTable")
        val Table = spark.textFile("Table")
        val sample = skewTable.sample(false, 0.3, 9).groupByKey().collect()
        val maxrowKey = sample.map(rows => (rows._2.size, rows._1)).maxBy(rows =>
            (rows._1)._2)
        val maxKeySkewedTable = skewTable.filter(row => {
            buildSideKeyGenerator(row) == maxrowKey
        })
        val mainSkewedTable = skewTable.filter(row => {
            !(buildSideKeyGenerator(row_ == maxrowKey))
        })
        val maxKeyJoinedRDD = maxKeySkewedTable.join(Table)
        val mainJoinedRDD = mainSkewedTable.join(Table)
        sc.union(maxKeyJoinedRDD, mainJoinedRDD)
    }
}