package com.joydeep.MovieRecomendationEngineExample

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.ml.evaluation.RegressionEvaluator

import java.io.{FileNotFoundException, IOException}

case class Rating(userId: Int, movieId: Int, rating: Float, timeStamp: Long)

object RecommendMovieApp extends App{
  val master  = "local"
  val name = "RecommendMovieApp"
  val spark = SparkSession.builder.
    master(master).
    appName(name).
    config("spark.app.id", name).   // To silence Metrics warning.
    getOrCreate()
  spark.conf.set("spark.executor.memory", "4g")

  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def parseString(str: String): Rating = {
    val field = str.split("\t")
    assert(field.size==4)
    Rating(field(0).toInt, field(1).toInt, field(2).toFloat, field(3).toLong)
  }

  try {
    // Create the rdd of the s3 csv data.
    val lines = spark.read.textFile("data/u.data")
    val ratings = lines.map(parseString).toDF()
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Create the ALS class instance and train the data.
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)
    println("model created")

    // get the predictions
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)
    predictions.show()

    // run the evaluation.
    val eval = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val rmse = eval.evaluate(predictions)
    println(f"The evaluation is: $rmse \n")

    val userRecs = model.recommendForAllUsers(10)
    println("showing userrecs")
    userRecs.show()
    //val recommendations_for_user = userRecs.filter(userRecs("userId").equalTo(471))
    //recommendations_for_user.show()
    val path = "result/myrecmodel"
    model.save(path)
    //val samemodel = ALSModel.load(path)
    //println("done")


  } catch {
    case e: FileNotFoundException => e.printStackTrace()
    case e: IOException => e.printStackTrace()
  } finally {
    spark.stop()
  }
}
