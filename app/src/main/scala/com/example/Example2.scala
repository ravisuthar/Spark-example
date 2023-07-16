package com.example

import org.apache.spark.sql.SparkSession

object Example2 {

  def main(args: Array[String]): Unit = {
    println("started")

    val sparkSession = SparkSession
      .builder
      .appName("Apache Spark")
      .master("local[*]")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")

    val list = 1 to 1000
    val rdd = sparkSession.sparkContext.parallelize(list, 2)
    val value = rdd.map(x => x * 2).sum()
    println("value: " + value)
    println("rdd.getNumPartitions: " + rdd.getNumPartitions)

    sparkSession.stop()
    println("completed")
  }

}
