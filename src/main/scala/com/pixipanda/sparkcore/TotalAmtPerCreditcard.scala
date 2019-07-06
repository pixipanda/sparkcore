package com.pixipanda.sparkcore

import org.apache.spark.sql.SparkSession

object TotalAmtPerCreditcard {

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


      val ccAmtPairRdd = transactionRdd.map(record => {
        val creditcardTransaction = CreditcardTransaction.parse(record)
        (creditcardTransaction.cc_num, creditcardTransaction.amt)
      })

      val totalAmtPerCreditcard = ccAmtPairRdd.reduceByKey(_ + _)

      totalAmtPerCreditcard.saveAsTextFile(outputPath)

  }

}