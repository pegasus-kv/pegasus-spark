package com.xiaomi.infra.pegasus.spark.analyser.sample.parquet

import com.xiaomi.infra.pegasus.spark.analyser.PegasusContext
import com.xiaomi.infra.pegasus.spark.core.Config
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object ConvertParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkStatCleanJob")
      .master("local[1]")
      .getOrCreate()

    val config = new Config()
      .setDestination(
        "",
        "80")
      .setDBInfo("c3srv-browser", "alchemy_feed_exchange_record")

    val pc = new PegasusContext(spark.sparkContext)
    val rdd = pc.pegasusSnapshotRDD(config)

    // please make sure your data can be convert valid string value
    val dataFrame = spark.createDataFrame(
      rdd.map(i =>
        Row(new String(i.hashKey), new String(i.sortKey), new String(i.value))),
      Schema.struct)

    dataFrame
      .coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      // in online, you need save on hdfs
      .save("c3srv-browser_alchemy_feed_exchange_record.parquet")
  }

}
