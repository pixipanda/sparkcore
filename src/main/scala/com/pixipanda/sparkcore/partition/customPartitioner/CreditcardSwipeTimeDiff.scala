package com.pixipanda.sparkcore.partition.customPartitioner

import com.pixipanda.sparkcore.CreditcardTransaction
import org.apache.spark.sql.SparkSession


object CreditcardSwipeTimeDiff {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val inputPath = args(1)



    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Load Credit card data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()



    val dataRdd = sparkSession.sparkContext.textFile(inputPath)
    val transactionRdd = dataRdd.map(CreditcardTransaction.parse)


    val pairRdd = transactionRdd.keyBy(transaction => CCTransTimeKey(transaction.cc_num, transaction.transTime))


    val parentNumPartitions = pairRdd.partitions.length

    val repartitionedRdd = pairRdd.repartitionAndSortWithinPartitions(new CreditcardPartitioner(parentNumPartitions))


    val timeDiffCCSwipeRdd = repartitionedRdd.mapPartitions(iter => {

      var previousKey = CCTransTimeKey("", null)
      for(currentKey <- iter) yield {
        val timediff =  if(currentKey._1.cc_num == previousKey.cc_num && previousKey.transTime != null) {
          currentKey._1.transTime.getTime - previousKey.transTime.getTime
        }
        else 0L
        previousKey = currentKey._1
        val diffSeconds = timediff / 1000 % 60
        val diffMinutes = timediff / (60 * 1000) % 60
        val diffHours = timediff / (60 * 60 * 1000)
        (currentKey._1.cc_num, diffHours+"hrs:" +diffMinutes +"mins:" + diffSeconds + "s")
      }

    })

    timeDiffCCSwipeRdd.foreach(record => {
      println("Creditcard: " + record._1 + " timediff: " + record._2)
    })
  }
}