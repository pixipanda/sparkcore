package com.pixipanda.sparkcore.cache

import com.pixipanda.sparkcore.CreditcardTransaction
import org.apache.spark.sql.SparkSession



object NonFraudAggregations {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)
    val totalAmtPerCredicardOutput = args(2)
    val totalAmtPerMerchantOutput = args(3)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val dataRdd = sparkSession.sparkContext.textFile(inputPath)
    val transactionRdd = dataRdd.map(CreditcardTransaction.parse)

    val genuineTransactionRdd = transactionRdd.filter(_.isFraud == 0).cache()


    val totalAmtPerCreditcard = genuineTransactionRdd.map(transaction => {
      (transaction.cc_num, transaction.amt)
    }).reduceByKey(_ + _)

    totalAmtPerCreditcard.saveAsTextFile(totalAmtPerCredicardOutput)

    scala.io.StdIn.readLine()


    val totalAmtPerMerchant = genuineTransactionRdd.map(transaction => {
      (transaction.merchant, transaction.amt)
    }).reduceByKey(_ + _)

    totalAmtPerMerchant.saveAsTextFile(totalAmtPerMerchantOutput)

  }
}