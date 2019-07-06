package com.pixipanda.sparkcore

import java.util.Date


case class CreditcardTransaction(cc_num:String, transId:String, transTime:Date, category:String, merchant:String, amt:Double, merchLatitude:Double, merchLongitude:Double, isFraud:Int)

object CreditcardTransaction {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")

  def parse(transactionRecord: String) = {

    val fields = transactionRecord.split(",")
    val cc_num = fields(0)
    val transId = fields(1)
    val transTime = format.parse(fields(2))
    val category = fields(3)
    val merchant = fields(4)
    val amt = fields(5).toDouble
    val lat = fields(6).toDouble
    val long = fields(7).toDouble
    val fraud = fields(8).toInt

    new CreditcardTransaction(cc_num, transId, transTime, category, merchant, amt, lat, long, fraud)
  }
}