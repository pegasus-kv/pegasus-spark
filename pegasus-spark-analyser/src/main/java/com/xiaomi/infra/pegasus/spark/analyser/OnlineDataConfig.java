package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.client.ScanOptions;
import com.xiaomi.infra.pegasus.spark.utils.FlowController.RateLimiterConfig;

public class OnlineDataConfig implements Config {

  private String clusterName;
  private String tableName;
  private int partitionCount;

  private ClientOptions clientOptions;
  private ScanOptions scanOptions;
  private RateLimiterConfig rateLimiterConfig;

  public OnlineDataConfig(ClientOptions clientOptions, String clusterName, String tableName) {
    this.clientOptions = clientOptions;
    this.tableName = tableName;
    this.clusterName = clusterName;
    this.partitionCount = Integer.MAX_VALUE;
    this.scanOptions = new ScanOptions();
    this.rateLimiterConfig = new RateLimiterConfig();
  }

  /**
   * set cluster name
   *
   * @param clusterName
   * @return
   */
  public OnlineDataConfig setClusterName(String clusterName) {
    this.clusterName = clusterName;
    return this;
  }

  /**
   * set table name
   *
   * @param tableName
   * @return
   */
  public OnlineDataConfig setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  /**
   * set except `result partition count`. if set value greater than `table partition count`, {@link
   * OnlineDataReader#getPartitionCount()} still return `table partition count`, default is
   * Integer.MAX_VALUE means results always split into `table partition count` partition
   *
   * @param partitionCount
   * @return
   */
  public OnlineDataConfig setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
    return this;
  }

  /**
   * set pegasus client options used for connecting pegasus online cluster, detail see {@link
   * ClientOptions}
   *
   * @param clientOptions
   * @return
   */
  public OnlineDataConfig setClientOptions(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    return this;
  }

  /**
   * set pegasus scanner options used for creating pegasus scanner, detail see {@link ScanOptions}
   *
   * @param scanOptions
   * @return
   */
  public OnlineDataConfig setScanOptions(ScanOptions scanOptions) {
    this.scanOptions = scanOptions;
    return this;
  }

  /**
   * set RateLimiter config to control request flow that include `qpsLimiter` and `bytesLimiter`,
   * detail see {@link com.xiaomi.infra.pegasus.spark.utils.FlowController} and {@link
   * RateLimiterConfig}
   *
   * @param rateLimiterConfig see {@link RateLimiterConfig}
   * @return this
   */
  public OnlineDataConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    return this;
  }

  @Override
  public DataType getDataType() {
    return DataType.ONLINE_DATA;
  }

  @Override
  public String getClusterName() {
    return clusterName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public ClientOptions getClientOptions() {
    return clientOptions;
  }

  public ScanOptions getScanOptions() {
    return scanOptions;
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
  }
}
