package com.xiaomi.infra.pegasus.spark.analyser.sample.parquet

import org.apache.spark.sql.SparkSession

object ParquetTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkStatCleanJob")
      .master("local[1]")
      .getOrCreate()

    val dataFrame = spark.read.parquet("c3srv-browser_alchemy_feed_exchange_record.parquet")
    dataFrame.show(10)
  }
}
