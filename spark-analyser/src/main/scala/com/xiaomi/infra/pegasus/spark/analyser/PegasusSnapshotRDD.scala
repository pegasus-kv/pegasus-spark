package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.spark.core.{Config, PegasusRecord}
import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.rocksdb.RocksDB

/**
 * PegasusContext is a serializable container for analysing Pegasus's checkpoint on HDFS.
 *
 * [[PegasusContext]] should be created in the driver, and shared with executors
 * as a serializable field.
 */
class PegasusContext(private val sc: SparkContext) extends Serializable {

  def pegasusSnapshotRDD(config: Config, dateTime: String = ""): PegasusSnapshotRDD = {
    //only simple match. if still invalid, it will not be matched successfully in FDService
    assert(
      dateTime.equals("") || dateTime.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"),
      "the date time format is error!")
    new PegasusSnapshotRDD(this, sc, config, dateTime)
  }
}

/**
 * A RDD backed by a FDS snapshot of Pegasus.
 *
 * To construct a PegasusSnapshotRDD, use [[PegasusContext#pegasusSnapshotRDD]].
 */
class PegasusSnapshotRDD private[analyser] (pegasusContext: PegasusContext,
                                            @transient sc: SparkContext,
                                            config: Config,
                                            dateTime: String)
  extends RDD[PegasusRecord](sc, Nil) {

  private val LOG = LogFactory.getLog(classOf[PegasusSnapshotRDD])

  private val coldBackUpData: BackUpData =
    if (dateTime.equals(""))
      new BackUpData(config)
    else new BackUpData(config, dateTime)

  override def compute(split: Partition,
                       context: TaskContext): Iterator[PegasusRecord] = {
    // Loads the librocksdb library into jvm.
    RocksDB.loadLibrary()

    LOG.info(
      "Create iterator for \"%s\" \"%s\" [pid: %d]"
        .format(config.DBCluster, config.DBTableName, split.index)
    )
    new PartitionIterator(context, config, coldBackUpData, split.index)
  }

  override protected def getPartitions: Array[Partition] = {
    val indexes = Array.range(0, coldBackUpData.getPartitionCount)
    indexes.map(i => {
      new PegasusPartition(i)
    })
  }

  def getPartitionCount: Int = {
    coldBackUpData.getPartitionCount
  }

  /**
   * @param other the other PegasusSnapshotRDD with which to diff against
   * @return a RDD representing a different set of records in which none of each
   *         exists in both `this` and `other`.
   */
  def diff(other: PegasusSnapshotRDD): RDD[PegasusRecord] = {
    subtract(other).union(other.subtract(this))
  }
}

/**
 * @param partitionIndex Each spark partition maps to one Pegasus partition.
 */
private class PegasusPartition(partitionIndex: Int) extends Partition {
  override def index: Int = partitionIndex
}