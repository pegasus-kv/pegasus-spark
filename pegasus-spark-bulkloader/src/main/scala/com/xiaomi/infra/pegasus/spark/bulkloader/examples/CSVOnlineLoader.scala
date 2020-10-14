package com.xiaomi.infra.pegasus.spark.bulkloader.examples

import java.time.Duration

import com.xiaomi.infra.pegasus.client.{ClientOptions, SetItem}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.bulkloader.OnlineLoaderConfig
import com.xiaomi.infra.pegasus.spark.utils.FlowController.RateLimiterConfig
import org.apache.spark.{SparkConf, SparkContext}

object CSVOnlineLoader {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName("Convert to Online data into into cluster")
      .setIfMissing("spark.master", "local[1]")
    val sc = new SparkContext(conf)

    sc.textFile("data.csv")
      .map(i => {
        val lines = i.split(",")
        new SetItem(
          lines(0).getBytes(),
          lines(1).getBytes(),
          lines(2).getBytes()
        )
      })
      .saveAsPegasusFile(
        new OnlineLoaderConfig(
          ClientOptions
            .builder()
            .metaServers(
              "10.232.52.164:34601,10.232.52.164:34602,10.232.52.164:34603"
            )
            .operationTimeout(Duration.ofMillis(10000))
            .build(),
          "onebox",
          "usertable"
          // Note: The finalQPS = QPS / partitionCount * parallelism. And `partitionCount` is usually
          // equal with `parallelism` generally in spark
        ).setRateLimiterConfig(new RateLimiterConfig().setQps(10000))
      )
  }
}
