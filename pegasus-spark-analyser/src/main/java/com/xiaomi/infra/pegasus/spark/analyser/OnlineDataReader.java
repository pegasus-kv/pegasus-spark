package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusClientInterface;
import com.xiaomi.infra.pegasus.client.PegasusScannerInterface;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.utils.FlowController;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OnlineDataReader implements PegasusReader {
  private static final Log LOG = LogFactory.getLog(OnlineDataReader.class);

  private OnlineDataConfig onlineDataConfig;
  private int partitionCount;

  public OnlineDataReader(OnlineDataConfig onlineDataConfig) throws PException {
    this.onlineDataConfig = onlineDataConfig;
    this.partitionCount =
        PegasusClientFactory.getSingletonClient(onlineDataConfig.getClientOptions())
            .getUnorderedScanners(
                onlineDataConfig.getTableName(),
                onlineDataConfig.getPartitionCount(),
                onlineDataConfig.getScanOptions())
            .size();
  }

  @Override
  public int getPartitionCount() {
    return partitionCount;
  }

  @Override
  public OnlineDataConfig getConfig() {
    return onlineDataConfig;
  }

  @Override
  public PegasusScanner getScanner(int pid) throws PegasusSparkException {
    FlowController flowController =
        new FlowController(partitionCount, onlineDataConfig.getRateLimiterConfig());

    try {
      PegasusClientInterface client =
          PegasusClientFactory.createClient(onlineDataConfig.getClientOptions());
      List<PegasusScannerInterface> scanners =
          client.getUnorderedScanners(
              onlineDataConfig.getTableName(),
              onlineDataConfig.getPartitionCount(),
              onlineDataConfig.getScanOptions());
      return new OnlineDataScanner(client, scanners.get(pid), flowController);
    } catch (PException e) {
      throw new PegasusSparkException("get pegasus online scanner error:", e);
    }
  }

  static class OnlineDataScanner implements PegasusScanner {
    private static final byte[] EMPTY = "".getBytes();

    private PegasusClientInterface client;
    private PegasusScannerInterface scanner;
    private FlowController flowController;
    private Pair<Pair<byte[], byte[]>, byte[]> result;

    public OnlineDataScanner(
        PegasusClientInterface client,
        PegasusScannerInterface scanner,
        FlowController flowController) {
      this.client = client;
      this.scanner = scanner;
      this.flowController = flowController;
      // init result EMPTY is for flowController to get the size at first
      this.result = Pair.of(Pair.of(EMPTY, EMPTY), EMPTY);
    }

    @Override
    public boolean isValid() {
      return result != null;
    }

    @Override
    public void seekToFirst() {}

    @Override
    public void next() {
      try {
        // `flowControl` initialized by `RateLimiterConfig` whose `qps` and `bytes` are both 0
        // default, which means if you don't set custom config value > 0 , it will not limit and
        // return immediately
        flowController.acquireQPS();
        flowController.acquireBytes(
            result.getKey().getLeft().length
                + result.getKey().getRight().length
                + result.getRight().length);

        result = scanner.next();
      } catch (PException e) {
        LOG.warn("get the next result error:", e);
      }
    }

    @Override
    public void close() {
      scanner.close();
      client.close();
    }

    @Override
    public PegasusRecord restore() {
      // TODO(jiashuo) PegasusScannerInterface don't return ttl, so expireTime is set 0 by default
      return new PegasusRecord(
          result.getLeft().getKey(), result.getLeft().getKey(), result.getValue(), 0);
    }
  }
}
