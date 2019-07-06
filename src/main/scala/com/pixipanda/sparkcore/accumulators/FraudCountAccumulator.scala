package com.pixipanda.sparkcore.accumulators

import com.pixipanda.sparkcore.CreditcardTransaction
import org.apache.spark.sql.SparkSession


object FraudCountAccumulator {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val fraudCountAcc = sparkSession.sparkContext.longAccumulator("FraudCount")
    
    val dataRdd = sparkSession.sparkContext.textFile(inputPath)
    val transactionRdd = dataRdd.map(CreditcardTransaction.parse)

    transactionRdd.foreach(transaction => {
      if (transaction.isFraud == 1)
        fraudCountAcc.add(1)
    })

    println("Fraud Count: " + fraudCountAcc.value)

  }
}