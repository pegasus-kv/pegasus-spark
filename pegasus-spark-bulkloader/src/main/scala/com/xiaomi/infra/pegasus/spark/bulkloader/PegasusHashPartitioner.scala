package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.spark.Tools
import org.apache.spark.Partitioner

class PegasusHashPartitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    Tools
      .remainderUnsigned(Tools.hash(key.asInstanceOf[RocksDBRecord].key), num)
      .toInt
  }
}
