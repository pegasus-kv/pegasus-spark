package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class Config implements Serializable {

  public String destinationUrl;
  public String destinationPort = "80";

  public String dbClusterName;
  public String dbTableName;
  public int dbTableId;
  public int dbTablePartitionCount;

  public int dbMaxFileOpenCounter = 50;
  public long dbReadAheadSize = 1024 * 1024L;

  public String dbColdBackUpPolicy = "every_day";
  public String dbColdBackupDateTime;

  // for bulkloader: whether to distinct source data
  public boolean isDistinct = true;

  public Config setDestination(String url, String port) {
    this.destinationUrl = url;
    this.destinationPort = port;
    return this;
  }
  
  public Config setDBInfo(
      String dbClusterName, String dbTableName, int dbTableId, int dbTablePartitionCount) {
    this.dbClusterName = dbClusterName;
    this.dbTableName = dbTableName;
    this.dbTableId = dbTableId;
    this.dbTablePartitionCount = dbTablePartitionCount;
    return this;
  }

  public Config setDBInfo(String dbCluster, String dbTableName) {
    this.dbClusterName = dbCluster;
    this.dbTableName = dbTableName;
    return this;
  }

  public Config setReadOption(int dbMaxFileOpenCounter, long dbReadAheadSize) {
    this.dbMaxFileOpenCounter = dbMaxFileOpenCounter;
    this.dbReadAheadSize = dbReadAheadSize;
    return this;
  }

  public Config setDbColdBackUpPolicy(String dbColdBackUpPolicy) {
    this.dbColdBackUpPolicy = dbColdBackUpPolicy;
    return this;
  }

  public Config setDbColdBackupDateTimey(String dbColdBackupDateTime) {
    this.dbColdBackupDateTime = dbColdBackupDateTime;
    return this;
  }

  public Config setDistinct(boolean distinct) {
    this.isDistinct = distinct;
    return this;
  }

}
