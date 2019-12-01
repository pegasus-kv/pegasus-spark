package com.xiaomi.infra.pegasus.spark.bulkloader

import CustomImplicits._
import com.xiaomi.infra.pegasus.spark.core.{Config, RocksDBRecord}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.rocksdb.RocksDB

import scala.collection.JavaConverters._

class RocksDBRDD(rdd: RDD[(Array[Byte], Array[Byte], Array[Byte])]) {

  def saveAsSSTFile(config: Config): Unit = {
    val recordRDD = rdd.map(i=>{
      (RocksDBRecord.create(i._1,i._2,i._3),"1")
    })

    val sstRDD = if (config.isDistinct) {
      recordRDD.distinct(config.DBTablePartitionCount).repartitionAndSortWithinPartitions(new PegasusHashPartitioner(sstConfig.DBTablePartitionCount))
    } else recordRDD.repartitionAndSortWithinPartitions(new PegasusHashPartitioner(config.DBTablePartitionCount))

    sstRDD.foreachPartition(i=>{
      RocksDB.loadLibrary()
      new BulkLoadData(config, i.asJava,TaskContext.getPartitionId()).write()
    })
  }

}