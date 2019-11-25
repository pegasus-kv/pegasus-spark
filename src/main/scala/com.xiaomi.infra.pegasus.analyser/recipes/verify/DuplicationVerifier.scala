package com.xiaomi.infra.pegasus.analyser.recipes.verify

import com.typesafe.config.{ConfigException, ConfigFactory}
import com.xiaomi.infra.pegasus.analyser._
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}

class DuplicationVerifierOptions {
  var tableName: String = ""
  var cluster1: String = ""
  var cluster2: String = ""

  var sparkMaster: String = "local"

  /**
    * Where you want to save the different records as text file.
    */
  var diffSetTextFileLocation: String = _
}

/**
  * Verifies if the user-specified two Pegasus checkpoints contain the identical data set.
  * The checkpoint may come from different clusters, locates on different FDS addresses.
  * This tool is used to confirm data from source cluster has been duplicated to remote destination cluster.
  */
class DuplicationVerifier(opts: DuplicationVerifierOptions) {

  val options: DuplicationVerifierOptions = opts
  private val LOG = LogFactory.getLog(classOf[DuplicationVerifier])

  class Result {
    var differences: Long = 0
    var same: Long = 0
  }

  def verify(): Result = {
    val conf = new SparkConf()
      .setAppName(
        "Verification of \"%s\" in clusters \"%s\" and \"%s\""
          .format(options.tableName, options.cluster1, options.cluster2)
      )
      .setIfMissing("spark.master", options.sparkMaster)
    val sc = new SparkContext(conf)

    val pc = new PegasusContext(sc)
    val rdd1 = pc.pegasusSnapshotRDD(options.cluster1, options.tableName)
    val rdd2 = pc.pegasusSnapshotRDD(options.cluster2, options.tableName)
    val partitionCount1 = rdd1.getPartitionCount
    val partitionCount2 = rdd2.getPartitionCount
    if (partitionCount1 != partitionCount2) {
      throw new IllegalArgumentException(
        "partition count of the table \"%s\" are different [cluster=\"%s\", partitionCount=\"%d\"] vs [cluster=\"%s\", partitionCount=\"%d\"]"
          .format(
            options.tableName,
            options.cluster1,
            partitionCount1,
            options.cluster2,
            partitionCount2
          )
      )
    }

    val diffSet = rdd1.subtract(rdd2).union(rdd2.subtract(rdd1))
    if (options.diffSetTextFileLocation != null && !options.diffSetTextFileLocation.isEmpty) {
      LOG.info("Save diffSet to text file: " + options.diffSetTextFileLocation)
      diffSet.saveAsTextFile(options.diffSetTextFileLocation)
    }

    val res = new Result
    res.differences = diffSet.count()
    res.same = (rdd1.count() + rdd2.count() - diffSet.count()) / 2
    res
  }
}

object DuplicationVerifier {
  def loadFromConfiguration(configPath: String): DuplicationVerifier = {
    val options = new DuplicationVerifierOptions()
    val config = ConfigFactory.load(configPath)
    options.cluster1 = config.getString("cluster1")
    options.cluster2 = config.getString("cluster2")
    options.tableName = config.getString("table-name")
    try {
      options.sparkMaster = config.getString("spark-master")
    } catch {
      case e: ConfigException =>
        println("spark-master is not configured, use local")
    }
    try {
      options.diffSetTextFileLocation =
        config.getString("diffset-text-file-location")
    } catch {
      case e: ConfigException =>
        println("diffset-text-file-location is not configured, use null")
    }
    new DuplicationVerifier(options)
  }
}

object VerifyDuplication {
  def main(args: Array[String]): Unit = {
    val verifier =
      DuplicationVerifier.loadFromConfiguration(args.apply(0))
    val result = verifier.verify()
    printf(
      "Verified snapshots of table \"%s\" in cluster1=\"%s\" and cluster2=\"%s\"\n",
      verifier.options.tableName,
      verifier.options.cluster1,
      verifier.options.cluster2
    )
    printf("Differences: %d\n", result.differences)
    printf("Same: %d\n", result.same)
  }
}
