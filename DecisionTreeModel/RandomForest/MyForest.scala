import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

object MyForest {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("MyForest")
        val sc = new SparkContext(conf)

        //由原始数据生成LabeledPoint数据
        val rawData = sc.textFile("hdfs://master:9000/user/ds/covtype.data")

        val data = rawData.map { line => 
            val values = line.split(',').map(_.toDouble)
            val featureVector = Vectors.dense(values.init)   // init返回出最后一个值之外的所有值，最后一列是目标
            val label = values.last - 1                       // 决策树要求label从0开始，所以要-1
            LabeledPoint(label, featureVector)
        }

        val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
        trainData.cache()
        cvData.cache()
        testData.cache()

        val forest = RandomForest.trainClassifier(
            trainData, 7, Map(10 -> 4, 11 -> 40), 20,
            "auto", "entropy", 30, 300)

        val metrics = getMetrics(forest, cvData)

        println("=================================================\n" + metrics.confusionMatrix)
        println("=================================================\n" + metrics.precision)
        sc.stop()

    }

    //生成随机森林模型
    def getMetrics(model: RandomForestModel, data: RDD[LabeledPoint]):
        MulticlassMetrics = {
        val predictionsAndLabels = data.map(example =>
        (model.predict(example.features), example.label)
        )
        new MulticlassMetrics(predictionsAndLabels)
    }

}

