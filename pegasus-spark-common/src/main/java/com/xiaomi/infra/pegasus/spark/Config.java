package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class Config implements Serializable {

  private static final long UNIT = 1024 * 1024L;

  public String destinationUrl;
  public String destinationPort;

  public String clusterName;
  public String tableName;
  public int tableId;
  public int tablePartitionCount;

  public int dbMaxFileOpenCounter = 50;
  public long dbReadAheadSize = 1024 * 1024L;

  public Config setDestination(String url, String port) {
    this.destinationUrl = url;
    this.destinationPort = port;
    return this;
  }

  public Config setDbInfo(String clusterName, String tableName) {
    this.clusterName = clusterName;
    this.tableName = tableName;
    return this;
  }

  public Config setTableId(int tableId) {
    this.tableId = tableId;
    return this;
  }

  public Config setTablePartitionCount(int tablePartitionCount) {
    this.tablePartitionCount = tablePartitionCount;
    return this;
  }

  public Config setDbMaxFileOpenCounter(int dbMaxFileOpenCounter) {
    this.dbMaxFileOpenCounter = dbMaxFileOpenCounter;
    return this;
  }

  public Config setDbReadAheadSize(long dbReadAheadSize) {
    this.dbReadAheadSize = dbReadAheadSize * UNIT;
    return this;
  }
}
