package com.xiaomi.infra.pegasus.spark.bulkloader.sample

import com.xiaomi.infra.pagasus.bulkloader.{DataLoader, SSTConfig}
import com.xiaomi.infra.pegasus.bulkloader.{CustomImplicits, PegasusRDD}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.rocksdb.RocksDB
import CustomImplicits._


object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    //System.setProperty("java.security.krb5.conf", "/home/mi/krb5.conf")

    val conf = new SparkConf()
      .setAppName("pegasus data analyse")
      .setIfMissing("spark.master", "local[1]")
      .set("spark.executor.instances", "8")

    val sc = new SparkContext(conf)
    //val data = sc.textFile("hdfs://zjyprc-hadoop/tmp/pegasus-dev/jiashuo/data.csv")
    val sstConfig = new SSTConfig()
      .setFDSUrl("")
      .setFDSPort("80")
      .setDBCluster("C1")
      .setDBTableName("T1")
      .setDBTableId(20)
      .setDBTablePartitionCount(8)
      .setDistinct(true)
    val data = sc.textFile("data.csv").map(i => {
      val lines = i.split(",")
      (lines(0), lines(1), lines(2))
    }).saveAsSSTFile(sstConfig)
  }

}
