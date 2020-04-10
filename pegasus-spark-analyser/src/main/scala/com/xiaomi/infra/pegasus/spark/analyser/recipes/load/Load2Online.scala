package com.xiaomi.infra.pegasus.spark.analyser.recipes.load

import com.xiaomi.infra.pegasus.client.SetItem
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.analyser.{
  ColdBackupConfig,
  ColdBackupLoader,
  WriteConfig,
  PegasusContext
}
import com.xiaomi.infra.pegasus.tools.Tools

object Load2Online {

  private final val COLD_BACKUP_FS_URL = ""
  private final val COLD_BACKUP_FS_PORT = ""
  private final val COLD_BACKUP_CLUSTER_NAME = ""
  private final val COLD_BACKUP_TABLE_NAME = ""
  private final val COLD_BACKUP_POLICY_NAME = ""

  private final val WRITE_META_SERVER =
    "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603"
  private final val WRITE_CLUSTER_NAME = "onebox"
  private final val WRITE_TABLE_NAME = "stat"
  private final val WRITE_TIMEOUT = 1000
  private final val WRITE_BULK_NUM = 100
  private final val WRITE_TTL_THRESHOLD = 1 * 24 * 3600 //1day

  def main(args: Array[String]): Unit = {
    val coldBackupConfig: ColdBackupConfig = new ColdBackupConfig()
    coldBackupConfig
      .setPolicyName(COLD_BACKUP_POLICY_NAME)
      .setRemote(COLD_BACKUP_FS_URL, COLD_BACKUP_FS_PORT)
      .setTableInfo(COLD_BACKUP_CLUSTER_NAME, COLD_BACKUP_TABLE_NAME)

    val writeConfig: WriteConfig = WriteConfig()
      .setMetaServer(WRITE_META_SERVER)
      .setCluster(WRITE_CLUSTER_NAME)
      .setTable(WRITE_TABLE_NAME)
      .setTimeout(WRITE_TIMEOUT)
      .setBulkNum(WRITE_BULK_NUM)
      .setTTLThreshold(WRITE_TTL_THRESHOLD)

    val conf: SparkConf = new SparkConf()
      .setAppName(
        "Convert to Online data into \"%s\" in clusters \"%s\""
          .format(writeConfig.cluster, writeConfig.table)
      )
      .setIfMissing("spark.master", "local[1]")
    val sc = new SparkContext(conf)

    new PegasusContext(sc)
      .pegasusSnapshotRDD(new ColdBackupLoader(coldBackupConfig))
      .map(i => {
        new SetItem(
          i.hashKey,
          i.sortKey,
          i.value,
          (i.expireTs - Tools.epoch_now()).toInt
        )
      })
      .saveAsOnlineData(writeConfig)
  }
}
