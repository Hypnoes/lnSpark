import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.featrue.VectorIndexer
import org.apache.ml.regression.{ GBTRegressionModel, GBTREgressor }

import org.apache.spark.sql.SparkSession

object CusEpPlA {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName(s"${this.getClass.getSimpleName}")
            .getOrCreate()
        import spark.implicits._

        val root = "hdfs://10.170.31.120:9000/user/hypnoes/"
        
        val df = spark.read.csv(root + args(1))
        
        val featureIndexer = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4)                // <-- {4}
            .fit(df)
        
        val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

        val gbt = new GBTREgressor()
            .setLabelCol("label")
            .setFeaturesCol("indexedFeatures")
            .setMaxIter(10)

        val pipeline = new Pipeline()
            .setStage(Array(featureIndexer, gbt))

        val model = pipeline.fit(trainingData)

        val predictions = model.transform(testData)

        predictions.select("prediction", "label", "features").write.csv(root + args(2))

        val evaluator = new RegressionEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("rmse")
        val rmse = evaluator.evaluate(predictions)
        println("root Mean Squared Error (RMSE) on test data = " + rmse)

        val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
        println("Learned regression GBT model:\n" + gbtModel.toDebugString)

        spark.stop()
    }
}

