package com.xiaomi.infra.pegasus.spark.bulkloader.sample

import com.xiaomi.infra.pegasus.spark.bulkloader.{
  BulkLoaderConfig,
  RocksDBRecord
}
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    //System.setProperty("java.security.krb5.conf", "/home/mi/krb5.conf")

    val conf = new SparkConf()
      .setAppName("pegasus data bulkloader")
      .setIfMissing("spark.master", "local[1]")

    val sc = new SparkContext(conf)

    val config = new BulkLoaderConfig()

    config
      .setDistinct(false)
      .setRemote("", "","","","")
      .setTableInfo("C2", "T2")
      // TODO tableId and partitionCount should be get just by clusterName and tableName
      .setTableId(20)
      .setTablePartitionCount(32)

    // Note(jiashuo1): if the partition size > 2G before "saveAsPegasusFile", you need
    // sc.textFile("data.csv").repartition(n), and let the partition size < 2G
    sc.textFile("data.csv")
      .map(i => {
        val lines = i.split(",")
        (RocksDBRecord
          .create(lines(0), lines(1), lines(2)),
          "")
      })
      .saveAsPegasusFile(config)
  }

}
