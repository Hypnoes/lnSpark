//准确度评估

import org.apache.spark.rdd._

def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
    val countsByCategory = data.map(_.label).countByValue()         // 计算数据中每个类别的样本数
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)    // 对类别的样本数进行排序并取出样本数
    counts.map(_.toDouble / counts.sum)
}

val trainPriorProbabilities = classProbabilities(trainData)
val cvPriorProbabilities = classProbabilities(cvData)
trainPriorProbabilities.zip(cvPriorProbabilities).map {             // 把训练集和CV集中的某个类别的概率结对然后相乘相加
    case (trainProb, cvProb) => trainProb * cvProb
}.sum

