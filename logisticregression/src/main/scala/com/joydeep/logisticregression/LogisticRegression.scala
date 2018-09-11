package com.joydeep.logisticRegression

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._

import java.io.{FileNotFoundException, IOException}

case class StumbleUpon(
  numberOfLinks: Double, numwords_in_url: Double,
  parametrizedLinkRatio: Double, spelling_errors_ratio: Double,
  label: Int
)

object LogisticRegressionApp extends App{
  val master  = "local"
  val name = "LogisticRegressionApp"
  val spark = SparkSession.builder.
    master(master).
    appName(name).
    config("spark.app.id", name).   // To silence Metrics warning.
    getOrCreate()
  spark.conf.set("spark.executor.memory", "4g")

  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  try {
    // Create the rdd of the s3 csv data.
    val rawData = sc.textFile("data/stumble_upon/train.tsv")
    val header = rawData.first()
    val rawData1 = rawData.filter(line => line != header)
    val records = rawData1.map(line => line.split("\t"))

    val data = records.map{ r =>
      val trimmed = r.map(_.replaceAll("\"", ""))
      val label = trimmed(r.size - 1).toInt
      val features = trimmed.slice(22, r.size - 1).map(d => if (d == "?") 0.0 else d.toDouble)
      assert(features.size == 4)
      StumbleUpon(features(0), features(1), features(2), features(3), label)
    }

    val dataDF = data.toDF()

    val featureCols = Array("numberOfLinks", "numwords_in_url", "parametrizedLinkRatio", "spelling_errors_ratio")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(dataDF)

    val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
    val model = lr.fit(trainingData)
    val predictions = model.transform(testData)

    val trainingSummary  = model.summary
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)

    val fMeasure = binarySummary.fMeasureByThreshold
    val fm = fMeasure.col("F-Measure")
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
    model.setThreshold(bestThreshold)

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val accuracy = evaluator.evaluate(predictions)
    println(accuracy)

  } catch {
    case e: FileNotFoundException => e.printStackTrace()
    case e: IOException => e.printStackTrace()
  } finally {
    spark.stop()
  }
}
