package com.pixipanda.sparkcore.partition.customPartitioner

import java.util.Date


case class CCTransTimeKey(cc_num:String, transTime:Date)


object CCTransTimeKey  {
  implicit def orderingByCCTransTime[A <: CCTransTimeKey]: Ordering[A] = {
    Ordering.by(pk => (pk.cc_num, pk.transTime))
  }
}