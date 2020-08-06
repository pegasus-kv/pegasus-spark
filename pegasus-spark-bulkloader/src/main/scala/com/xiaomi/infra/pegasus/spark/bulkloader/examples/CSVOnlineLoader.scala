package com.xiaomi.infra.pegasus.spark.bulkloader.examples

import com.xiaomi.infra.pegasus.client.SetItem
import com.xiaomi.infra.pegasus.spark.bulkloader.OnlineLoadConfig
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.bulkloader.CustomImplicits._

object CSVOnlineLoader {

  private final val WRITE_META_SERVER =
    "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603"
  private final val WRITE_CLUSTER_NAME = "onebox"
  private final val WRITE_TABLE_NAME = "stat"
  private final val WRITE_TIMEOUT = 10000
  private final val WRITE_BULK_NUM = 100
  private final val WRITE_TTL_THRESHOLD = 1 * 24 * 3600 //1day

  def main(args: Array[String]): Unit = {

    val writeConfig: OnlineLoadConfig = OnlineLoadConfig()
      .metaServer(WRITE_META_SERVER)
      .clusterName(WRITE_CLUSTER_NAME)
      .tableName(WRITE_TABLE_NAME)
      .timeout(WRITE_TIMEOUT)
      .bulkNum(WRITE_BULK_NUM)
      .TTLThreshold(WRITE_TTL_THRESHOLD)

    val conf: SparkConf = new SparkConf()
      .setAppName(
        "Convert to Online data into \"%s\" in clusters \"%s\""
          .format(writeConfig.clusterName, writeConfig.tableName)
      )
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
      .saveAsPegasusFile(writeConfig)
  }
}
