package com.example

import org.apache.spark.sql.SparkSession

object Example1 {
  def main(args: Array[String]): Unit = {

    println("started")

    val sparkSession = SparkSession
      .builder
      .appName("Apache Spark")
      .master("local[*]")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val list = List("john", "marry", "pitor", "ram", "jessy")
    val rdd = sparkSession.sparkContext.parallelize(list, 1)
    val value = rdd.map(e => e.toUpperCase())
    value.collect().foreach(println)

    sparkSession.stop()
    println("completed")
  }

}
