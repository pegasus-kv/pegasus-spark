package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.client.SetItem
import com.xiaomi.infra.pegasus.spark.analyser.recipes.convertOnlineData.PegasusOnlineRDD
import org.apache.spark.rdd.RDD

object CustomImplicits {

  implicit def convertFromByte(rdd: RDD[SetItem]): PegasusOnlineRDD =
    new PegasusOnlineRDD(rdd)
}
