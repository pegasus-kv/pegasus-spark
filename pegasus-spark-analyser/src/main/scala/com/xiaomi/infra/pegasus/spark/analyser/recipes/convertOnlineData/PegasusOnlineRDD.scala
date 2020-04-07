package com.xiaomi.infra.pegasus.spark.analyser.recipes.convertOnlineData

import java.time.Duration
import com.xiaomi.infra.pegasus.tools.FlowController
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

case class OnlineConfig() extends Serializable {
  var metaServer: String = _
  var timeout: Long = _
  var asyncWorks: Int = _

  var cluster: String = _
  var table: String = _

  // flow control：
  // single_qps = flowControl * bulkNum
  // total_qps = single_qps * partitionNum(here partitionNum equal count of partition in
  // running, also is count of parallel jobs, which isn't more than total pegasus partition count)
  var bulkNum: Int = 10
  var flowControl: Int = 5

  def setMetaServer(metaServer: String): OnlineConfig = {
    this.metaServer = metaServer
    this
  }

  def setTimeout(timeout: Long): OnlineConfig = {
    this.timeout = timeout
    this
  }

  def setAsyncWorks(asyncWorks: Int): OnlineConfig = {
    this.asyncWorks = asyncWorks
    this
  }

  def setCluster(cluster: String): OnlineConfig = {
    this.cluster = cluster
    this
  }

  def setTable(table: String): OnlineConfig = {
    this.table = table
    this
  }

  def setBulkNum(bulkNum: Int): OnlineConfig = {
    this.bulkNum = bulkNum
    this
  }

  def setFlowControl(flowControl: Int): OnlineConfig = {
    this.bulkNum = bulkNum
    this
  }

}

class PegasusOnlineRDD(resource: RDD[SetItem]) extends Serializable {

  private val logger = LoggerFactory.getLogger(classOf[PegasusOnlineRDD])

  def saveAsOnlineData(onlineConfig: OnlineConfig): Unit = {
    resource.foreachPartition(i => {
      var count = 0

      val client = PegasusClientFactory.getSingletonClient(
        ClientOptions
          .builder()
          .metaServers(onlineConfig.metaServer)
          .operationTimeout(Duration.ofMillis(onlineConfig.timeout))
          .build()
      )

      val flowController = new FlowController(onlineConfig.flowControl)
      val index = TaskContext.getPartitionId()
      i.sliding(onlineConfig.bulkNum, onlineConfig.bulkNum)
        .foreach(slice => {
          flowController.getToken()
          var success = false
          while (!success) {
            try {
              client.batchSet(onlineConfig.table, slice.asJava)
              success = true
            } catch {
              case ex: PException =>
                logger
                  .info("partition index " + index + ": batchSet error:" + ex)
                Thread.sleep(10)
            }
            count += slice.size
            if (count % 10000 == 0) {
              logger
                .info("partition index " + index + ": batchSet count:" + count)
            }
          }
        })
      logger.info(
        "partition index " + index + ": total batchSet count:" + count
      )
      flowController.stop()
      client.close()
    })
  }
}
