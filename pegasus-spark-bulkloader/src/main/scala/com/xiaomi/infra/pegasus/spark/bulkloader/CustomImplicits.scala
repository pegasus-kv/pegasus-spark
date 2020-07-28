package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.Tools
import org.apache.spark.rdd.RDD

/**
  * custom implicits object, you can:
  *
  * import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
  *
  * to use it.
  */
object CustomImplicits {

  /**
    * The implicit implement of ordering by PegasusBytes
    */
  implicit val basePegasusKey: Ordering[PegasusKey] =
    new Ordering[PegasusKey] {
      override def compare(x: PegasusKey, y: PegasusKey): Int = {
        Tools.compare(x.data, y.data)
      }
    }

  /**
    * The implicit method of converting RDD[(PegasusBytes,PegasusBytes) to PegasusRecordRDD
    * @param rdd
    * @return
    */
  implicit def convert2PegasusRecordRDD(
      rdd: RDD[(PegasusKey, PegasusValue)]
  ): PegasusRecordRDD =
    new PegasusRecordRDD(rdd)
}
