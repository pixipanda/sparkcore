package com.pixipanda.sparkcore.partition

import org.apache.spark.sql.SparkSession


object ParallelizePartition {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val sc = sparkSession.sparkContext

    val rdd = sc.parallelize(1 to 10, 5)


    println("Default parallelism: " + sc.defaultParallelism)
    println("Number of partitions: " + rdd.getNumPartitions)
    println("Partitioner: " + rdd.partitioner)

    val glomRdd = rdd.glom().map(a => a.toList)
    println("Partitions structure: " + glomRdd.collect().toList)

  }
}