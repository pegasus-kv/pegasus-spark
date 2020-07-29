package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;

public class BulkLoaderConfig extends CommonConfig {

  private String dataPathRoot = "/pegasus-bulkloader";
  // todo(jiashuo): tableId, tablePartitionCount should be get by clusterName and tableName
  private int tableId;
  private int tablePartitionCount;

  public BulkLoaderConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    super(hdfsConfig, clusterName, tableName);
  }

  public BulkLoaderConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
  }

  /**
   * table id
   *
   * @param tableId
   * @return this
   */
  public BulkLoaderConfig setTableId(int tableId) {
    this.tableId = tableId;
    return this;
  }

  /**
   * table partition count
   *
   * @param tablePartitionCount
   * @return this
   */
  public BulkLoaderConfig setTablePartitionCount(int tablePartitionCount) {
    this.tablePartitionCount = tablePartitionCount;
    return this;
  }

  /**
   * set the bulkloader data root path, default is "/pegasus-bulkloader"
   *
   * @param dataPathRoot data path root
   * @return this
   */
  public BulkLoaderConfig setDataPathRoot(String dataPathRoot) {
    this.dataPathRoot = dataPathRoot;
    return this;
  }


  public String getDataPathRoot() {
    return dataPathRoot;
  }

  public int getTableId() {
    return tableId;
  }

  public int getTablePartitionCount() {
    return tablePartitionCount;
  }

}
