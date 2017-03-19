// 构造决策树模型
import org.apache.saprk.mllib.evaluation._
import org.apache.saprk.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._

def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]):
    MulticlassMetrics = {
    val predictionsAndLabels = data.map(example => 
    (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
}

val model = DecisionTree.trainClassifier(
    trainData, 7, Map[Int, Int](), "gini", 4, 100)

val metrics = getMetrics(model, cvData)

metrics.confusionMatrix
metrics.precision

def getMetrics(model: RandomForestModel, data: RDD[LabeledPoint]):
    MulitclassMetrics = {
    val predictionsAndLabels = data.map(example =>
    (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
}