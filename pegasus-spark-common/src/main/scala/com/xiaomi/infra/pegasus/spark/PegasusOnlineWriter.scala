package com.xiaomi.infra.pegasus.spark

import java.time.Duration

import com.xiaomi.infra.pegasus.tools.{FlowController, Tools}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import com.xiaomi.infra.pegasus.client.{
  ClientOptions,
  PException,
  PegasusClientFactory,
  SetItem
}

case class OnlineWriteConfig() extends Serializable {
  var metaServer: String = _
  var timeout: Long = 1000
  var asyncWorks: Int = 4

  var cluster: String = _
  var table: String = _

  // ttlThreshold(unit is seconds) must not be negative. less than the threshold won't be write,
  // default is 0.
  var ttlThreshold: Int = 0
  // flow control：
  // single_qps = flowControl * bulkNum
  // total_qps = single_qps * partitionNum(here partitionNum equal count of partition in
  // running, also is count of parallel jobs, which isn't more than total pegasus partition count)
  var bulkNum: Int = 10
  var flowControl: Int = 5

  def metaServer(metaServer: String): OnlineWriteConfig = {
    this.metaServer = metaServer
    this
  }

  def timeout(timeout: Long): OnlineWriteConfig = {
    this.timeout = timeout
    this
  }

  def asyncWorks(asyncWorks: Int): OnlineWriteConfig = {
    this.asyncWorks = asyncWorks
    this
  }

  def clusterName(cluster: String): OnlineWriteConfig = {
    this.cluster = cluster
    this
  }

  def tableName(table: String): OnlineWriteConfig = {
    this.table = table
    this
  }

  def bulkNum(bulkNum: Int): OnlineWriteConfig = {
    this.bulkNum = bulkNum
    this
  }

  def flowControl(flowControl: Int): OnlineWriteConfig = {
    this.flowControl = flowControl
    this
  }

  def TTLThreshold(ttl: Int): OnlineWriteConfig = {
    this.ttlThreshold = ttl
    this
  }

}

class PegasusOnlineWriter(resource: RDD[SetItem]) extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[PegasusOnlineWriter])

  def saveAsOnlineData(writeConfig: OnlineWriteConfig): Unit = {
    resource.foreachPartition(i => {
      var totalCount = 0
      var validCount = 0
      var expireCount = 0

      val client = PegasusClientFactory.getSingletonClient(
        ClientOptions
          .builder()
          .metaServers(writeConfig.metaServer)
          .asyncWorkers(writeConfig.asyncWorks)
          .operationTimeout(Duration.ofMillis(writeConfig.timeout))
          .build()
      )

      val flowController = new FlowController(writeConfig.flowControl)
      val index = TaskContext.getPartitionId()
      i.sliding(writeConfig.bulkNum, writeConfig.bulkNum)
        .foreach(slice => {
          flowController.getToken()
          val validData =
            slice.filter(i => i.ttlSeconds >= writeConfig.ttlThreshold)
          var success = false
          while (!success) {
            try {
              client.batchSet(writeConfig.table, validData.asJava)
              success = true
            } catch {
              case ex: PException =>
                logger
                  .info("partition index " + index + ": batchSet error:" + ex)
                Thread.sleep(10)
            }
            totalCount += slice.size
            validCount += validData.size
            expireCount = totalCount - validCount
            if (totalCount % 10000 == 0) {
              logger
                .info(
                  "partition index " + index + " is running: totalCount = " + totalCount +
                    ", validCount:" + validCount + ", expireCount:" + expireCount +
                    ", expireRatio:" + (expireCount.toDouble / totalCount)
                    .formatted("%.2f")
                )
            }
          }
        })
      logger
        .info(
          "partition index " + index + " has completed: totalCount = " + totalCount +
            ", validCount:" + validCount + ", expireCount:" + expireCount +
            ", expireRatio:" + (expireCount.toDouble / totalCount)
            .formatted("%.2f")
        )

      flowController.stop()
      PegasusClientFactory.closeSingletonClient()
    })
  }
}
