package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusClientInterface;
import com.xiaomi.infra.pegasus.client.SetItem;
import com.xiaomi.infra.pegasus.spark.FlowController;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OnlineLoader {
  private static final Log LOG = LogFactory.getLog(OnlineLoader.class);

  private PegasusClientInterface client;
  private FlowController flowController;
  private OnlineLoaderConfig onlineLoaderConfig;

  public OnlineLoader(OnlineLoaderConfig config, int partitionTaskCount) throws PException {
    this.onlineLoaderConfig = config;
    this.client = PegasusClientFactory.getSingletonClient(config.getClientOptions());
    if (config.getRateLimiterConfig() != null) {
      long qps = config.getRateLimiterConfig().getQps();
      long megabytes = config.getRateLimiterConfig().getMegabytes();
      double factor = config.getRateLimiterConfig().getBurstFactor();

      this.flowController =
          new FlowController(partitionTaskCount, factor)
              .withMBytesLimiter(megabytes)
              .withQPSLimiter(qps);
    }
  }

  public void load(List<SetItem> setItems) throws InterruptedException {
    boolean success = false;
    int failedCount = 0;
    while (!success) {
      try {
        if (flowController != null) {
          int bytes = 0;
          for (SetItem setItem : setItems) {
            bytes += setItem.hashKey.length + setItem.sortKey.length + setItem.value.length;
          }
          flowController.acquireQPS(setItems.size());
          flowController.acquireBytes(bytes);
        }

        client.batchSet(onlineLoaderConfig.getTableName(), setItems);
        success = true;
      } catch (PException e) {
        LOG.info("batchSet error(" + failedCount + "):" + e);
        Thread.sleep(100);
      }
    }
  }

  public void close() throws PException {
    PegasusClientFactory.closeSingletonClient();
  }
}
