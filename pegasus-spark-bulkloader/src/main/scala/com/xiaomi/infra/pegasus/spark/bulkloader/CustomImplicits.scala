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
  implicit val basePegasusKey: Ordering[PegasusBytes] =
    new Ordering[PegasusBytes] {
      override def compare(x: PegasusBytes, y: PegasusBytes): Int = {
        Tools.compare(x.data, y.data)
      }
    }

  /**
    * The implicit method of converting RDD[(PegasusBytes,PegasusBytes) to PegasusRecordRDD
    * @param rdd
    * @return
    */
  implicit def convert2PegasusRecordRDD(
      rdd: RDD[(PegasusBytes, PegasusBytes)]
  ): PegasusRecordRDD =
    new PegasusRecordRDD(rdd)
}
