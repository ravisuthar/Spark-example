package com.example

import org.apache.spark.sql.SparkSession

object Example3 {

  def main(args: Array[String]): Unit = {

    var spark = SparkSession
      .builder
      .appName("chapter2")
      .master("local[*]")
      .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.13:10.2.0")

      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val mongodf = spark.read.format("mongodb")
      .option("uri", "mongodb://localhost:27017")
      .option("database", "latest_db")
      .option("collection", "students")
      .load()

    mongodf.printSchema()
    mongodf.show()
    mongodf.createOrReplaceTempView("tempMongo")
    spark.sql("SELECT * FROM tempMongo").show()

    spark.stop()
    println("completed")
  }

}
