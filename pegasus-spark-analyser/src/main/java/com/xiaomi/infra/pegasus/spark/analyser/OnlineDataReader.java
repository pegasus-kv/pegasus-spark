package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusClientInterface;
import com.xiaomi.infra.pegasus.client.PegasusScannerInterface;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public class OnlineDataReader implements PegasusReader {
  OnlineDataConfig onlineDataConfig;
  PegasusClientInterface client;
  List<PegasusScannerInterface> scanners;

  public OnlineDataReader(OnlineDataConfig onlineDataConfig) throws PException {
    this.onlineDataConfig = onlineDataConfig;
    this.client = PegasusClientFactory.createClient(onlineDataConfig.getClientOptions());
    this.scanners =
        client.getUnorderedScanners(
            onlineDataConfig.getTableName(),
            onlineDataConfig.getPartitionCount(),
            onlineDataConfig.getScanOptions());
  }

  @Override
  public int getPartitionCount() {
    return onlineDataConfig.partitionCount;
  }

  @Override
  public OnlineDataConfig getConfig() {
    return onlineDataConfig;
  }

  @Override
  public PegasusScanner getScanner(int pid) {
    return new OnlineDataScanner(scanners.get(pid), client);
  }

  static class OnlineDataScanner implements PegasusScanner {
    PegasusScannerInterface scanner;
    PegasusClientInterface client;
    Pair<Pair<byte[], byte[]>, byte[]> result;

    public OnlineDataScanner(PegasusScannerInterface scanner, PegasusClientInterface client) {
      this.scanner = scanner;
      this.client = client;
    }

    @Override
    public boolean isValid() {
      return true;
    }

    @Override
    public void seekToFirst() {}

    @Override
    public void next() {
      try {
        scanner.next();
      } catch (PException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void close() {
      scanner.close();
      client.close();
    }

    @Override
    public PegasusRecord restore() {
      return new PegasusRecord(
          result.getLeft().getKey(), result.getLeft().getKey(), result.getValue(), 0);
    }
  }
}
