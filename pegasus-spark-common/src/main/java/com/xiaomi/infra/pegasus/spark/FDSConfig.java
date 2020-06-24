package com.xiaomi.infra.pegasus.spark;

public class FDSConfig extends HDFSConfig {

  public String remoteFsBucketName;
  public String remoteFsEndPoint;
  public String remoteFsAccessKey;
  public String remoteFsAccessSecret;

  public FDSConfig(
      String accessKey, String accessSecret, String bucketName, String endPoint, String port)
      throws PegasusSparkException {
    super("fds://" + accessKey + ":" + accessSecret + "@" + bucketName + "." + endPoint, port);
    this.remoteFsAccessKey = accessKey;
    this.remoteFsAccessSecret = accessSecret;
    this.remoteFsBucketName = bucketName;
    this.remoteFsEndPoint = endPoint;
  }
}
