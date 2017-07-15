// 决策树调优
val evaluations = 
    for (impurity <- Array("gini", "entropy");
         depth    <- Array(1, 20);
         bins     <- Array(10, 300))
        yield {
            val model = DecisionTree.trainClassifier(
                trainData, 7, Map[Int, Int](), impurity, depth, bins)
            val predictionsAndLabels = cvData.map(example => 
             (model.predict(example.features), example.label)
            )
            val accuracy = 
              new MulticlassMetrics(predictionsAndLabels).precision
              ((impurity, depth, bins), accuracy)
        }

evaluations.sortBy(_._2).reverse.foreach(println)

// 减小过拟合
val data = rawData.map { line =>
  val values = line.split(',').map(_.toDouble)
  val wilderness = values.slice(10, 14).indexOf(1.0).toDouble
  val soil = value.slice(14, 54).indexOf(10).toDouble
  val featureVector = 
    Vectors.dense(values.slice(0, 10) :+ wilderness :+ soil)
  val label = values.last -1
  LabelPoint(label, featureVector)
}

val evaluations = 
    for (impurity <- Array("gini", "entropy");
         depth    <- Array(1, 20, 30);
         bins     <- Array(40, 300))
      yield {
        val model = DecisionTree.trainClassifier(
          trainData, 7, Map(10 -> 4, 11 -> 40),
          impurity, depth, bins)
        val trainAccuracy = getMetrics(model, trainData).precision
        val cvAccuracy = getMetrics(model, cvData).precision
        ((impurity, depth, bins), (trainAccuracy, cvAccuracy)
    }

// 随机森林
val forest = RandomForest.trainClassifier(
  trainData, 7, Map(10 -> 4, 11 -> 40), 20,
  "auto", "entropy", 30, 300)

val input = "2709, 125, 28, 67, 23, 3224, 253, 207, 61, 6094, 0, 29"
val vector = Vectors.dense(input.split(',').map(_.toDouble)
forest.predict(vector)

val evaluations = 
  for (impurity <- Array("gini", "entropy");
       depth    <- Array(1, 20, 30);
       bins     <- Array(40, 300))
    yield {
      val model = RandomForest.trainClassifier(
        trainData, 7, Map(10 -> 4, 11 -> 40), 20,
        "auto", impurity, depth, bins)
      val trainAccuracy = getMetrics(model, trainData).precision
      val cvAccuray = getMetrics(model, cvData).precision
      ((impurity, depth, bins), (trainAccuray, cvAccuracy)
    }

