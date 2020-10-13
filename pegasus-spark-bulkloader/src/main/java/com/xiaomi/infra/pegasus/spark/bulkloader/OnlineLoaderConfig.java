package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.spark.FlowController.RateLimiterConfig;

public class OnlineLoaderConfig {
  private ClientOptions clientOptions;
  private RateLimiterConfig rateLimiterConfig;
  private int ttlThreshold;
  private String clusterName;
  private String tableName;

  public OnlineLoaderConfig(ClientOptions clientOptions, String clusterName, String tableName) {
    this.clientOptions = clientOptions;
    this.clusterName = clusterName;
    this.tableName = tableName;
  }

  public OnlineLoaderConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    return this;
  }

  public OnlineLoaderConfig setTtlThreshold(int ttlThreshold) {
    this.ttlThreshold = ttlThreshold;
    return this;
  }

  public ClientOptions getClientOptions() {
    return clientOptions;
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
  }

  public int getTtlThreshold() {
    return ttlThreshold;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getTableName() {
    return tableName;
  }
}
