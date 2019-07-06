package com.pixipanda.sparkcore.accumulators.customAccumulator

import com.pixipanda.sparkcore.CreditcardTransaction
import org.apache.spark.sql.SparkSession


object TotalAmtPerCreditcardMain {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val inputPath = args(1)
    
    
    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()


    val creditcardAmtAcc = new CredicardAmtAccumulator()
    sparkSession.sparkContext.register(creditcardAmtAcc, "CreditcardAmtAccumulator")


    val dataRdd = sparkSession.sparkContext.textFile(inputPath)
    val transactionRdd = dataRdd.map(CreditcardTransaction.parse)

    transactionRdd.foreach(transaction => {
      creditcardAmtAcc.add((transaction.cc_num, transaction.amt))
    })

    println("CreditcardAmtAccumulator: " + creditcardAmtAcc.value)

  }
}