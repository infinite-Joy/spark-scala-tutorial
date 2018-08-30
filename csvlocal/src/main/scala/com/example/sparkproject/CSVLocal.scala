package com.example.sparkProject

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.{FileNotFoundException, IOException}

object CSVLocal extends App{
  val master  = "local"
  val name = "CSVLocal"
  val spark = SparkSession.builder.
    master(master).
    appName(name).
    config("spark.app.id", name).   // To silence Metrics warning.
    getOrCreate()

  val sc = spark.sparkContext
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  try {
    // Create the rdd of the s3 csv data.
    val lines = sc.textFile("data/C2ImportCalEventSample.csv")

    // filter out only the columns that we want.
    val filteredlines = lines.map(_.split(",")).map{x => (x(0), x(1))}

    // Build the dataframe
    val newNames = Seq("event", "place")
    val df = filteredlines.toDF(newNames: _*)

    df.show()
    
  } catch {
    case e: FileNotFoundException => e.printStackTrace()
    case e: IOException => e.printStackTrace()
  } finally {
    spark.stop()
  }
}
