package com.xiaomi.infra.pegasus.spark.bulkloader.sample

import com.xiaomi.infra.pegasus.spark.core.Config
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object CSVBulkLoader {

  def main(args: Array[String]): Unit = {
    //System.setProperty("java.security.krb5.conf", "/home/mi/krb5.conf")

    val conf = new SparkConf()
      .setAppName("pegasus data analyse")
      .setIfMissing("spark.master", "local[1]")
      .set("spark.executor.instances", "8")

    val sc = new SparkContext(conf)

    val sstConfig = new Config()
      .setDestination("", "80")
      .setDBInfo("C2", "T2", 20, 32)
      .setDistinct(false)

    //val data = sc.textFile("hdfs://zjyprc-hadoop/tmp/pegasus-dev/jiashuo/data.csv")
    sc.textFile("data.csv")
      .map(i => {
        val lines = i.split(",")
        (lines(0), lines(1), lines(2))
      })
      .saveAsSSTFile(sstConfig)
  }

}
