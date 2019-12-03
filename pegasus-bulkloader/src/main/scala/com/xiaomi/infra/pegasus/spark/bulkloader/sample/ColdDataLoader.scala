package com.xiaomi.infra.pegasus.spark.bulkloader.sample

import com.xiaomi.infra.pegasus.spark.analyser.PegasusContext
import com.xiaomi.infra.pegasus.spark.core.Config
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object ColdDataLoader {

  def main(args: Array[String]): Unit = {
    //System.setProperty("java.security.krb5.conf", "/home/mi/krb5.conf")

    val conf = new SparkConf()
      .setAppName("pegasus data bulkloader")
      .setIfMissing("spark.master", "local[1]")
      .set("spark.executor.instances", "8")

    val sc = new SparkContext(conf)
    //val data = sc.textFile("hdfs://zjyprc-hadoop/tmp/pegasus-dev/jiashuo/data.csv")
    val sstConfig = new Config()
      .setDestination("", "80")
      .setDBInfo("C2", "T2", 20, 32)
      .setDistinct(false)

    val pegasusConfig = new Config()
      .setDestination("", "80")
      .setDBInfo("c4tst-fd", "injection0")
      .setDBColdBackUpPolicy("500g")
    val pc = new PegasusContext(sc)
    val rdd = pc.pegasusSnapshotRDD(pegasusConfig)

    rdd
      .map(i => {
        (i.hashKey, i.sortKey, i.value)
      })
      .saveAsSSTFile(sstConfig)
  }

}
