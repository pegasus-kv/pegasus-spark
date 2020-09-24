package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.client.ScanOptions;
import com.xiaomi.infra.pegasus.spark.FlowController.RateLimiterConfig;

public class OnlineDataConfig implements Config {

  String tableName;
  int partitionCount;
  ClientOptions clientOptions;
  ScanOptions scanOptions;
  RateLimiterConfig rateLimiterConfig;

  public OnlineDataConfig(ClientOptions clientOptions, String tableName) {
    this.clientOptions = clientOptions;
    this.tableName = tableName;
  }

  public OnlineDataConfig setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public OnlineDataConfig setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
    return this;
  }

  public OnlineDataConfig setClientOptions(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    return this;
  }

  public OnlineDataConfig setScanOptions(ScanOptions scanOptions) {
    this.scanOptions = scanOptions;
    return this;
  }

  public OnlineDataConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    return this;
  }

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

  @Override
  public DataType getDataType() {
    return DataType.ONLINE_DATA;
  }
}
