package com.pixipanda.sparkcore.accumulators.customAccumulator

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.Map


class CredicardAmtAccumulator extends  AccumulatorV2[(String, Double), Map[String, Double]] {

  val creditcardAmt = Map.empty[String, Double]

  def reset() = {
    creditcardAmt.clear()
  }

  //Accumulator Aggregation logic must be written in add method. This will be evaluated
  //by each task in the Executor
  def add(keyValue: (String, Double)): Unit = {

    val credCardNumber = keyValue._1
    val incrementedAmt = keyValue._2  + creditcardAmt.getOrElse(credCardNumber, 0.0)

    creditcardAmt.put(credCardNumber, incrementedAmt)
  }


  def isZero(): Boolean = {
    creditcardAmt.isEmpty
  }


  // Merge method is called in the driver. It will merge all local accumulators.
  def merge(x: AccumulatorV2[(String, Double), Map[String, Double]]): Unit = {

    x.value.foreach(add)

  }

  def copy() = new CredicardAmtAccumulator


  def value() =  creditcardAmt

}