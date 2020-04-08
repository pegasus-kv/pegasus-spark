package com.xiaomi.infra.pegasus.spark.analyser.recipes.convertOnlineData

import com.xiaomi.infra.pegasus.client.SetItem
import org.apache.spark.{SparkConf, SparkContext}
import com.xiaomi.infra.pegasus.spark.analyser.CustomImplicits._
import com.xiaomi.infra.pegasus.spark.analyser.{
  ColdBackupConfig,
  ColdBackupLoader,
  PegasusContext
}
import com.xiaomi.infra.pegasus.tools.Tools

object ConvertOnlineData {

  private final val COLD_BACKUP_FS_URL = ""
  private final val COLD_BACKUP_FS_PORT = "80"
  private final val COLD_BACKUP_CLUSTER_NAME = ""
  private final val COLD_BACKUP_TABLE_NAME = ""
  private final val COLD_BACKUP_POLICY_NAME = ""

  private final val ONLINE_META_SERVER = ""
  private final val ONLINE_CLUSTER_NAME = ""
  private final val ONLINE_TABLE_NAME = ""
  private final val ONLINE_WRITE_TIMEOUT = 1000
  private final val ONLINE_BULK_NUM = 100

  def main(args: Array[String]): Unit = {
    val coldBackupConfig: ColdBackupConfig = new ColdBackupConfig()
    coldBackupConfig
      .setPolicyName(COLD_BACKUP_POLICY_NAME)
      .setRemote(COLD_BACKUP_FS_URL, COLD_BACKUP_FS_PORT)
      .setTableInfo(COLD_BACKUP_CLUSTER_NAME, COLD_BACKUP_TABLE_NAME)

    val onlineConfig: OnlineConfig = OnlineConfig()
      .setMetaServer(ONLINE_META_SERVER)
      .setCluster(ONLINE_CLUSTER_NAME)
      .setTable(ONLINE_TABLE_NAME)
      .setTimeout(ONLINE_WRITE_TIMEOUT)
      .setBulkNum(ONLINE_BULK_NUM)

    val conf: SparkConf = new SparkConf()
      .setAppName(
        "Convert to Online data into \"%s\" in clusters \"%s\""
          .format(onlineConfig.cluster, onlineConfig.table)
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
      .saveAsOnlineData(onlineConfig)
  }
}
