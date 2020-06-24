package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.Tools
import org.apache.spark.rdd.RDD

/**
  * custom implicits object, you can:
  *
  * import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
  *
  * to use it.
  *
  */
object CustomImplicits {

  /**
    * The implicit implement of ordering by RocksDBRecord
    */
  implicit val basePegasusKey: Ordering[RocksDBRecord] =
    new Ordering[RocksDBRecord] {
      override def compare(x: RocksDBRecord, y: RocksDBRecord): Int = {
        Tools.compare(x.key, y.key)
      }
    }

  /**
    * The implicit method of converting RDD[(RocksDBRecord,String) to RocksDBRDD
    * @param rdd
    * @return
    */
  implicit def convertFromByte(rdd: RDD[(RocksDBRecord, String)]): RocksDBRDD =
    new RocksDBRDD(rdd)
}
