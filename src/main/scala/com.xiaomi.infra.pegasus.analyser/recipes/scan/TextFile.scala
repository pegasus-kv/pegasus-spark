package com.xiaomi.infra.pegasus.analyser.recipes.scan

import com.xiaomi.infra.pegasus.analyser.PegasusContext
import org.apache.spark.{SparkConf, SparkContext}

object TextFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("scan data to csvFile")
      .setIfMissing("spark.master", "local[1]")

    val sc = new SparkContext(conf)
    val pc = new PegasusContext(sc)

    val rdd = pc.pegasusSnapshotRDD("alsgsrv-mixin", "mixin_offline_msg")
    rdd.saveAsTextFile("TextData")
    //or you can write single file
    //rdd.repartition(1).saveAsTextFile("TextData")
  }
}
