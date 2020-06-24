package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

/** The config is used for accessing the remote file system. user need init it for FDS or HDFS */
public class FSConfig implements Serializable {

  public String remoteFsUrl;
  public String remoteFsPort;

  public String remoteFsBucketName;
  public String remoteFsEndPoint;
  public String remoteFsAccessKey;
  public String remoteFsAccessSecret;

  public RocksDBOptions rocksDBOptions;

  /**
   * the constructor can be used for fds and hdfs
   *
   * @param remoteFsUrl
   * @param remoteFsPort
   * @throws PegasusSparkException
   */
  public FSConfig(String remoteFsUrl, String remoteFsPort) throws PegasusSparkException {
    this.remoteFsUrl = remoteFsUrl;
    this.remoteFsPort = remoteFsPort;
    this.rocksDBOptions = new RocksDBOptions(remoteFsUrl, remoteFsPort);
  }

  /**
   * the contructor only be used for fds
   *
   * @param accessKey
   * @param accessSecret
   * @param bucketName
   * @param endPoint
   * @param port
   * @throws PegasusSparkException
   */
  public FSConfig(
      String accessKey, String accessSecret, String bucketName, String endPoint, String port)
      throws PegasusSparkException {
    this("fds://" + accessKey + ":" + accessSecret + "@" + bucketName + "." + endPoint, port);
    this.remoteFsAccessKey = accessKey;
    this.remoteFsAccessSecret = accessSecret;
    this.remoteFsBucketName = bucketName;
    this.remoteFsEndPoint = endPoint;
  }
}
