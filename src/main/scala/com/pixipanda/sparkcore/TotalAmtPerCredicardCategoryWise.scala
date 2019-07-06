package com.pixipanda.sparkcore

import org.apache.spark.sql.SparkSession


object TotalAmtPerCredicardCategoryWise {

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

    val transactionRdd = sparkSession.sparkContext.textFile(inputPath)

    val creditCardCategoryAmtPairRdd = transactionRdd.map(record => {

      val transaction = CreditcardTransaction.parse(record)
      ((transaction.cc_num, transaction.category), transaction.amt)
    })

    val totalAmtPerCreditcard = creditCardCategoryAmtPairRdd.reduceByKey(_ + _)

    totalAmtPerCreditcard.saveAsTextFile(outputPath)
  }
}