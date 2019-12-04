package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class Config implements Serializable {

  public String destinationUrl;
  public String destinationPort = "80";

  public String DBCluster;
  public String DBTableName;
  public int DBTableId;
  public int DBTablePartitionCount;

  public int DBMaxFileOpenCounter = 50;
  public long DBReadAheadSize = 1024 * 1024L;

  public String DBColdBackUpPolicy = "every_day";

  public boolean isDistinct = true;

  public Config setDestination(String url, String port) {
    this.destinationUrl = url;
    this.destinationPort = port;
    return this;
  }

  public Config setDBInfo(
      String DBCluster, String DBTableName, int DBTableId, int DBTablePartitionCount) {
    this.DBCluster = DBCluster;
    this.DBTableName = DBTableName;
    this.DBTableId = DBTableId;
    this.DBTablePartitionCount = DBTablePartitionCount;
    return this;
  }

  public Config setDBInfo(String DBCluster, String DBTableName) {
    this.DBCluster = DBCluster;
    this.DBTableName = DBTableName;
    return this;
  }

  public Config setReadOption(int DBMaxFileOpenCounter, long DBReadAheadSize) {
    this.DBMaxFileOpenCounter = DBMaxFileOpenCounter;
    this.DBReadAheadSize = DBReadAheadSize;
    return this;
  }

  public Config setDBColdBackUpPolicy(String DBColdBackUpPolicy) {
    this.DBColdBackUpPolicy = DBColdBackUpPolicy;
    return this;
  }

  public Config setDistinct(boolean distinct) {
    this.isDistinct = distinct;
    return this;
  }
}
