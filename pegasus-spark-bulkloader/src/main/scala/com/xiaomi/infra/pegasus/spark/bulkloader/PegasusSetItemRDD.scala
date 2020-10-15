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

      var totalCount = 0
      val validData = i.filter(p => {
        totalCount += 1
        if (totalCount % 100000 == 0) {
          logger.info(
            "partition(" + partitionId + ") totalCount = " + totalCount
          )
        }
        p.ttlSeconds >= config.getTTLThreshold
      })

      var validCount = 0
      validData
        .sliding(config.getBatchCount, config.getBatchCount)
        .foreach(slice => {
          onlineLoader.load(slice.asJava)
          validCount += slice.size
          if (validCount % 100000 == 0) {
            logger
              .info("partition(" + partitionId + ") validCount = " + validCount)
          }
        })

      onlineLoader.close()
    })
  }

}
