package com.xiaomi.infra.pegasus.spark.bulkloader;

public class BulkLoadInfo {

  public String cluster;
  public int app_id;
  public String app_name;
  public int partition_count;

  public BulkLoadInfo(String cluster, String app_name, int app_id, int partition_count) {
    this.cluster = cluster;
    this.app_name = app_name;
    this.app_id = app_id;
    this.partition_count = partition_count;
  }
}
