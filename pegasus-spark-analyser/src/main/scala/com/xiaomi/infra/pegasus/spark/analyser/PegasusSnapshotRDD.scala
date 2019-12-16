package com.xiaomi.infra.pegasus.spark.analyser

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
// TODO(jiashuo1) refactor rdd/iterator for adding pegasus online data
class PegasusContext(private val sc: SparkContext) extends Serializable {

  def pegasusSnapshotRDD(config: ColdBackupConfig): PegasusSnapshotRDD = {
    //only simple match. if still invalid, it will not be matched successfully in ColdDataLoader
    assert(config.coldDataTime.equals("") || config.coldDataTime.matches(
             "[0-9]{4}-[0-9]{2}-[0-9]{2}"),
           "the date time format is error!")
    new PegasusSnapshotRDD(this, config, sc)
  }
}

/**
  * A RDD backed by a FDS snapshot of Pegasus.
  *
  * To construct a PegasusSnapshotRDD, use [[PegasusContext#pegasusSnapshotRDD]].
  */
class PegasusSnapshotRDD private[analyser] (pegasusContext: PegasusContext,
                                            config: ColdBackupConfig,
                                            @transient sc: SparkContext)
    extends RDD[PegasusRecord](sc, Nil) {

  private val LOG = LogFactory.getLog(classOf[PegasusSnapshotRDD])

  private val coldDataLoader: ColdBackupLoader = new ColdBackupLoader(config)

  override def compute(split: Partition,
                       context: TaskContext): Iterator[PegasusRecord] = {
    // Loads the librocksdb library into jvm.
    RocksDB.loadLibrary()

    LOG.info(
      "Create iterator for \"%s\" \"%s\" [pid: %d]"
        .format(config.clusterName, config.tableName, split.index)
    )
    new PartitionIterator(context, config, coldDataLoader, split.index)
  }

  override protected def getPartitions: Array[Partition] = {
    val indexes = Array.range(0, coldDataLoader.getPartitionCount)
    indexes.map(i => {
      new PegasusPartition(i)
    })
  }

  def getPartitionCount: Int = {
    coldDataLoader.getPartitionCount
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
