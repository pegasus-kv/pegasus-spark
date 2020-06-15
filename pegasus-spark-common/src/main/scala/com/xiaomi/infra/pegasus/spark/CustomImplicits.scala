package com.xiaomi.infra.pegasus.spark

import com.xiaomi.infra.pegasus.client.SetItem
import org.apache.spark.rdd.RDD

object CustomImplicits {

  implicit def convertFromSetItem(rdd: RDD[SetItem]): PegasusOnlineWriter =
    new PegasusOnlineWriter(rdd)
}
