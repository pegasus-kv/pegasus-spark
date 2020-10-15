package com.xiaomi.infra.pegasus.spark.analyser.examples.basic

import java.time.Duration

import com.xiaomi.infra.pegasus.client.ClientOptions
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.analyser.OnlineDataConfig
import com.xiaomi.infra.pegasus.spark.utils.FlowController.RateLimiterConfig
import org.apache.spark.{SparkConf, SparkContext}

object OnlineDataCounter {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("count pegasus data stored in Pegasus cluster")
      .setIfMissing("spark.master", "local[1]")

    val sc = new SparkContext(conf)
    val count = sc
      .pegasusRDD(
        new OnlineDataConfig(
          ClientOptions
            .builder()
            .metaServers("127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603")
            .operationTimeout(Duration.ofMillis(10000))
            .build(),
          "onebox",
          "usertable"
        ).setRateLimiterConfig(new RateLimiterConfig().setQps(1000))
      )
      .count()

    println(count)
  }

}
