import org.apache.spark._
import SprakContext._

object wordCount{
    val sc = new SparkContext(agrs(0), "wordCount",
        System.getenv("SPARK_HOME"),
        Seq(System.getenv("SPARK_TEST_JAR")))

    val textRDD = sc.textFile(args(1))
    val result = textRDD.faltMap{
        case(key, value) => value.toString().split("\\s+");
    }.map(word => (word, 1)).reduceByKey(_+_)

    result.saveAsSequenceFile(agrs(2))
}