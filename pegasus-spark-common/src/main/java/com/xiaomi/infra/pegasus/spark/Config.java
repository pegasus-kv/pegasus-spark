package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class Config implements Serializable {

  public String remoteFsUrl;
  public String remoteFsPort;

  public String clusterName;
  public String tableName;

  public RocksDBOptions rocksDBOptions;

  // todo(jiashuo): the FDS options will be refactored
  public String remoteFsAccessKey;
  public String remoteFsAccessSecret;
  public String remoteFsBucketName;
  public String remoteFsEndPoint;

  public Config(String remoteFsUrl, String remoteFsPort, String clusterName, String tableName)
      throws PegasusSparkException {
    this.remoteFsUrl = remoteFsUrl;
    this.remoteFsPort = remoteFsPort;
    this.clusterName = clusterName;
    this.tableName = tableName;

    this.rocksDBOptions = new RocksDBOptions(remoteFsUrl, remoteFsPort);
  }
}
