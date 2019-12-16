package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.Tools
import org.apache.spark.rdd.RDD

object CustomImplicits {
  implicit val basePegasusKey: Ordering[RocksDBRecord] =
    new Ordering[RocksDBRecord] {
      override def compare(x: RocksDBRecord, y: RocksDBRecord): Int = {
        Tools.compare(x.key, y.key)
      }
    }

  implicit def convertFromByte(rdd: RDD[(RocksDBRecord,String)]): RocksDBRDD = new RocksDBRDD(rdd)
}
