package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.core.{RocksDBRecord, Tools}
import org.apache.spark.rdd.RDD

object CustomImplicits {
  implicit val basePegasusKey: Ordering[RocksDBRecord] = new Ordering[RocksDBRecord] {
    override def compare(x: RocksDBRecord, y: RocksDBRecord): Int = {
      Tools.compare(x.key, y.key)
    }
  }

  implicit def convertFromByte(rdd: RDD[(Array[Byte], Array[Byte], Array[Byte])]): RocksDBRDD = new RocksDBRDD(rdd)

  implicit def convertFromString(rdd: RDD[(String, String, String)]): RocksDBRDD = new RocksDBRDD(rdd.map(i => (i._1.getBytes(), i._2.getBytes(), i._3.getBytes())))
}
