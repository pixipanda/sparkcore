package com.pixipanda.sparkcore.partition.customPartitioner

import org.apache.spark.Partitioner


class CreditcardPartitioner(partitions: Int) extends Partitioner {

  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[CCTransTimeKey]
    nonNegativeMod(k.cc_num.hashCode(), numPartitions)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

}