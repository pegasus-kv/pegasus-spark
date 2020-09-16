package com.xiaomi.infra.pegasus.spark;

import com.xiaomi.infra.pegasus.spark.CommonConfig.ClusterType;
import java.util.Objects;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class FDSConfig extends HDFSConfig {

  String bucketName;
  String endPoint;
  String accessKey;
  String accessSecret;

  public FDSConfig(
      String accessKey, String accessSecret, String bucketName, String endPoint, String port) {
    super("fds://" + accessKey + ":" + accessSecret + "@" + bucketName + "." + endPoint, port);
    this.accessKey = accessKey;
    this.accessSecret = accessSecret;
    this.bucketName = bucketName;
    this.endPoint = endPoint;
  }

  public FDSConfig(String accessKey, String accessSecret, String bucketName, String endPoint) {
    this(accessKey, accessSecret, bucketName, endPoint, "80");
  }

  public static FDSConfig loadFDSConfig(CommonConfig.ClusterType clusterType) throws ConfigurationException {
    XMLConfiguration configuration =
        new XMLConfiguration(
            Objects.requireNonNull(FDSConfig.class.getClassLoader().getResource("core-site.xml")));
    return loadFDSConfig(configuration, clusterType);
  }

  public static FDSConfig loadFDSConfig(XMLConfiguration configuration, CommonConfig.ClusterType clusterType) {
    String key = configuration.getString("fs.fds.key");
    String secret = configuration.getString("fs.fds.secret");
    String bucket = configuration.getString("fs.fds.bucket");
    String endpoint = configuration.getString("fs.fds.endpoint." + clusterType.toString());
    String port = configuration.getString("fs.fds.port");
    return new FDSConfig(key, secret, bucket, endpoint, port);
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public void setAccessSecret(String accessSecret) {
    this.accessSecret = accessSecret;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getEndPoint() {
    return endPoint;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getAccessSecret() {
    return accessSecret;
  }
}
