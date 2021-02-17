package com.xiaomi.infra.pegasus.spark.analyser

import com.xiaomi.infra.pegasus.spark.utils.JNILibraryLoader
import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * PegasusContext is a serializable container for analysing Pegasus's data on HDFS/FDS or on
  * Online Cluster.
  *
  * [[PegasusContext]] should be created in the driver, and shared with executors
  * as a serializable field.
  */
class PegasusContext(private val sc: SparkContext) extends Serializable {

  def pegasusRDD(config: Config): PegasusRDD = {

    new PegasusRDD(
      this,
      PegasusReaderFactory.createDataReader(config),
      sc
    )
  }
}

/**
  * A RDD backed by pegasus data, v1.1.0 to be released will support snapshot data in HDFS/FDS and
  * pegasus cluster online data
  *
  * To construct a PegasusRDD, use [[PegasusContext#pegasusSnapshotRDD]].
  */
class PegasusRDD private[analyser] (
    pegasusContext: PegasusContext,
    pegasusReader: PegasusReader,
    @transient sc: SparkContext
) extends RDD[PegasusRecord](sc, Nil) {

  private val LOG = LogFactory.getLog(classOf[PegasusRDD])

  override def compute(
      split: Partition,
      context: TaskContext
  ): Iterator[PegasusRecord] = {
    // Loads the librocksdb library into jvm.
    JNILibraryLoader.load()

    LOG.info(
      "Create iterator for \"%s\" \"%s\" [pid: %d]"
        .format(
          pegasusReader.getConfig.getClusterName,
          pegasusReader.getConfig.getTableName,
          split.index
        )
    )
    new PartitionIterator(context, pegasusReader, split.index)
  }

  override protected def getPartitions: Array[Partition] = {
    val indexes = Array.range(0, pegasusReader.getPartitionCount)
    indexes.map(i => {
      new PegasusPartition(i)
    })
  }

  def getPartitionCount: Int = {
    pegasusReader.getPartitionCount
  }

  /**
    * @param other the other PegasusRDD with which to diff against
    * @return a RDD representing a different set of records in which none of each
    *         exists in both `this` and `other`.
    */
  def diff(other: PegasusRDD): RDD[PegasusRecord] = {
    subtract(other).union(other.subtract(this))
  }
}

/**
  * @param partitionIndex Each spark partition maps to one Pegasus partition.
  */
private class PegasusPartition(partitionIndex: Int) extends Partition {
  override def index: Int = partitionIndex
}
