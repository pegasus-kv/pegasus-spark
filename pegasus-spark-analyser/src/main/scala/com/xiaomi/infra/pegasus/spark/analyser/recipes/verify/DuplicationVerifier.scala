package com.xiaomi.infra.pegasus.spark.analyser.recipes.verify

import com.xiaomi.infra.pegasus.spark.analyser.{ColdDataConfig, PegasusContext}
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Verifies if the user-specified two Pegasus checkpoints contain the identical data set.
  * The checkpoint may come from different clusters, locates on different FDS addresses.
  * This tool is used to confirm data from source cluster has been duplicated to remote destination cluster.
  */
class DuplicationVerifier() {

  private val LOG = LogFactory.getLog(classOf[DuplicationVerifier])

  class Result {
    var dbCluster1: String = _
    var dbTable1: String = _
    var dbCluster2: String = _
    var dbTable2: String = _

    var differences: Long = 0
    var same: Long = 0
    var numRdd1: Long = 0
    var numRdd2: Long = 0
  }

  def verify(): Result = {
    val coldDataConfig1 = new ColdDataConfig()
    val coldDataConfig2 = new ColdDataConfig()

    coldDataConfig1
      .setDestination(
        "",
        "")
      .setDbInfo("c3srv-browser", "alchemy_feed_exchange_record")

    coldDataConfig2
      .setDestination(
        "",
        "")
      .setDbInfo("c3srv-browser", "alchemy_feed_exchange_record")

    val conf = new SparkConf()
      .setAppName(
        "Verification of \"%s\" and \"%s\" in clusters \"%s\" and \"%s\""
          .format(
            coldDataConfig1.tableName,
            coldDataConfig2.tableName,
            coldDataConfig1.clusterName,
            coldDataConfig2.clusterName
          )
      )
      .setIfMissing("spark.master", "local[9]")
    val sc = new SparkContext(conf)

    val pc = new PegasusContext(sc)

    val rdd1 = pc.pegasusSnapshotRDD(coldDataConfig1)
    val rdd2 = pc.pegasusSnapshotRDD(coldDataConfig2)
    val partitionCount1 = rdd1.getPartitionCount
    val partitionCount2 = rdd2.getPartitionCount
    if (partitionCount1 != partitionCount2) {
      throw new IllegalArgumentException(
        "partition count of the table \"%s\" and \"%s\" are different [cluster=\"%s\", partitionCount=\"%d\"] vs [cluster=\"%s\", partitionCount=\"%d\"]"
          .format(
            coldDataConfig1.tableName,
            coldDataConfig2.tableName,
            coldDataConfig1.clusterName,
            partitionCount1,
            coldDataConfig2.clusterName,
            partitionCount2
          )
      )
    }

    val diffSet = rdd1.diff(rdd2)

    val res = new Result
    res.dbCluster1 = coldDataConfig1.clusterName
    res.dbTable1 = coldDataConfig1.tableName
    res.dbCluster2 = coldDataConfig2.clusterName
    res.dbTable2 = coldDataConfig2.tableName

    res.differences = diffSet.count()
    res.numRdd1 = rdd1.count()
    res.numRdd2 = rdd2.count()
    res.same = (res.numRdd1 + res.numRdd2 - res.differences) / 2
    res
  }
}

object VerifyDuplication {
  def main(args: Array[String]): Unit = {

    val result = new DuplicationVerifier().verify()
    printf(
      "Verified snapshots of table \"%s\" and \"%s\" in cluster1=\"%s\" and cluster2=\"%s\"\n",
      result.dbCluster1,
      result.dbTable1,
      result.dbCluster2,
      result.dbTable2
    )
    printf("Differences: %d\n", result.differences)
    printf("Same: %d\n", result.same)
    printf("Number of RDD1: %d\n", result.numRdd1)
    printf("Number of RDD2: %d\n", result.numRdd2)
  }
}
