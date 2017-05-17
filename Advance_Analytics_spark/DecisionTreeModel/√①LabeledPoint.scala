//由原始数据生成LabeledPoint数据

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._

val rawData = sc.textFile("hdfs:///user/ds/covtype.data")

val data = rawData.map { line => 
    val values = line.spit(',').map(_.toDouble)
    val featureVector = Vector.dense(values.init)   // init返回出最后一个值之外的所有值，最后一列是目标
    val lab = values.last - 1                       // 决策树要求label从0开始，所以要-1
    LabeledPoint(label, featureVector)
}

val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
trainData.cache()
cvData.cache()
testData.cache()