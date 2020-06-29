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
    * The implicit implement of ordering by PegasusRecord
    */
  implicit val basePegasusKey: Ordering[PegasusRecord] =
    new Ordering[PegasusRecord] {
      override def compare(x: PegasusRecord, y: PegasusRecord): Int = {
        Tools.compare(x.key, y.key)
      }
    }

  /**
    * The implicit method of converting RDD[(PegasusRecord,String) to PegasusRecordRDD
    * @param rdd
    * @return
    */
  implicit def convert2PegasusRecordRDD(
      rdd: RDD[(PegasusRecord, String)]
  ): PegasusRecordRDD =
    new PegasusRecordRDD(rdd)
}
