package com.pixipanda.sparkcore

import org.apache.spark.sql.SparkSession


object FraudTransactions {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)
    val outputPath = args(2)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val dataRdd = sparkSession.sparkContext.textFile(inputPath)

    val transactionRdd = dataRdd.map(CreditcardTransaction.parse)
    val fraudtransactionRdd = transactionRdd.filter(_.isFraud == 1)
    fraudtransactionRdd.saveAsTextFile(outputPath)

  }
}