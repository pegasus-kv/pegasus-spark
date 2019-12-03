package com.xiaomi.infra.pegasus.spark.analyser.sample

import com.xiaomi.infra.pegasus.spark.analyser._
import com.xiaomi.infra.pegasus.spark.core.Config
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Verifies if the user-specified two Pegasus checkpoints contain the identical data set.
 * The checkpoint may come from different clusters, locates on different FDS addresses.
 * This tool is used to confirm data from source cluster has been duplicated to remote destination cluster.
 */
class DuplicationVerifier(table1: Config, table2: Config) {

  private val LOG = LogFactory.getLog(classOf[DuplicationVerifier])

  class Result {
    var differences: Long = 0
    var same: Long = 0
    var numRdd1: Long = 0
    var numRdd2: Long = 0
  }

  def verify(): Result = {

    val conf = new SparkConf()
      .setAppName(
        "Verification of \"%s\" in clusters \"%s\" and \"%s\""
          .format(table1.DBTableName, table1.DBCluster, table2.DBCluster)
      )
      .setIfMissing("spark.master", "local[9]")

    val sc = new SparkContext(conf)
    val pc = new PegasusContext(sc)
    val rdd1 = pc.pegasusSnapshotRDD(table1)
    val rdd2 = pc.pegasusSnapshotRDD(table2)
    val partitionCount1 = rdd1.getPartitionCount
    val partitionCount2 = rdd2.getPartitionCount
    if (partitionCount1 != partitionCount2) {
      throw new IllegalArgumentException(
        "partition count of the table \"%s\" are different [cluster=\"%s\", partitionCount=\"%d\"] vs [cluster=\"%s\", partitionCount=\"%d\"]"
          .format(
            table1.DBTableName,
            table2.DBCluster,
            partitionCount1,
            table2.DBCluster,
            partitionCount2
          )
      )
    }

    val diffSet = rdd1.diff(rdd2)
    diffSet.saveAsTextFile("")

    val res = new Result
    res.differences = diffSet.count()
    res.numRdd1 = rdd1.count()
    res.numRdd2 = rdd2.count()
    res.same = (res.numRdd1 + res.numRdd2 - res.differences) / 2
    res
  }
}

object VerifyDuplication {
  def main(args: Array[String]): Unit = {
    val table1 = new Config()
      .setDestination("", "")
      .setDBInfo("C1", "T1")

    val table2 = new Config()
      .setDestination("", "")
      .setDBInfo("C2", "T2")

    val verifier = new DuplicationVerifier(table1, table2)
    val result = verifier.verify()
    printf(
      "Verified snapshots of table \"%s\" in cluster1=\"%s\" and cluster2=\"%s\"\n",
      table1.DBTableName,
      table1.DBCluster,
      table2.DBCluster
    )
    printf("Differences: %d\n", result.differences)
    printf("Same: %d\n", result.same)
    printf("Number of RDD1: %d\n", result.numRdd1)
    printf("Number of RDD2: %d\n", result.numRdd2)
  }
}
