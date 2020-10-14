package com.xiaomi.infra.pegasus.spark.bulkloader

import com.xiaomi.infra.pegasus.client.SetItem
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._

class PegasusSetItemRDD(resource: RDD[SetItem]) extends Serializable {
  private val logger = LoggerFactory.getLogger(classOf[PegasusSetItemRDD])

  def saveAsPegasusFile(config: OnlineLoaderConfig): Unit = {

    resource.foreachPartition(i => {
      val onlineLoader = new OnlineLoader(config, resource.getNumPartitions)
      val partitionId = TaskContext.getPartitionId

      val validData = i.filter(p => p.ttlSeconds >= config.getTtlThreshold)

      logger.info(
        "partition(" + partitionId + ") totalCount = " + i.size + ", validCount = "
          + validData.size + ", expireCount = " + (i.size - validData.size)
      )

      var successCount = 0
      validData
        .sliding(config.getBatchCount, config.getBatchCount)
        .foreach(slice => {
          onlineLoader.load(slice.asJava)

          successCount += slice.size

          logger.info(
            "partition(" + partitionId + ") successCount = " + successCount + "(" +
              (successCount.toDouble / validData.size).formatted("%.2f") + ")"
          )
        })

      onlineLoader.close()
    })
  }

}
