package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.spark.utils.FlowController.RateLimiterConfig;
import java.io.Serializable;

public class OnlineLoaderConfig implements Serializable {
  private ClientOptions clientOptions;
  private RateLimiterConfig rateLimiterConfig;

  private int ttlThreshold;
  private int batchCount;
  private String clusterName;
  private String tableName;

  public OnlineLoaderConfig(ClientOptions clientOptions, String clusterName, String tableName) {
    this.clientOptions = clientOptions;
    this.clusterName = clusterName;
    this.tableName = tableName;

    this.ttlThreshold = 0;
    this.batchCount = 10;
  }

  public OnlineLoaderConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    return this;
  }

  public OnlineLoaderConfig setTtlThreshold(int ttlThreshold) {
    this.ttlThreshold = ttlThreshold;
    return this;
  }

  public OnlineLoaderConfig setBatchCount(int batchCount) {
    this.batchCount = batchCount;
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

  public int getBatchCount() {
    return batchCount;
  }
}
