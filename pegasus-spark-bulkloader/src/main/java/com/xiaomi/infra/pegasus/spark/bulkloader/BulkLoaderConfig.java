package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import java.io.Serializable;

public class BulkLoaderConfig extends CommonConfig {
  private AdvancedConfig advancedConfig = new AdvancedConfig();

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

  /**
   * set AdvancedConfig decide the data whether to sort or distinct, detail see {@link
   * AdvancedConfig}
   *
   * @param advancedConfig
   * @return this
   */
  public BulkLoaderConfig setAdvancedConfig(AdvancedConfig advancedConfig) {
    this.advancedConfig = advancedConfig;
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

  public AdvancedConfig getAdvancedConfig() {
    return advancedConfig;
  }

  /**
   * This class supports two options: enableSort and enableDistinct. Pegasus bulkload require the
   * data must be sorted and distinct by [hashKeyLength][hashKey][sortKey]. if you make sure that
   * the source data has been sorted or distinct base the rule, you can set them false to ignored
   * the sort or distinct process to decrease the time consuming. Otherwise, you may not should use
   * the class generally.
   */
  public static class AdvancedConfig implements Serializable {

    private boolean isDistinct = true;
    private boolean isSort = true;

    /**
     * set whether to distinct the [hashKeyLength][hashKey][sortKey] of pegasus records generated by
     * resource data, please make sure the data has been distinct base the above rule, otherwise,
     * don't set false.
     *
     * @param distinct true or false, default is "true"
     * @return this
     */
    public AdvancedConfig enableDistinct(boolean distinct) {
      isDistinct = distinct;
      return this;
    }

    /**
     * set whether to sort the [hashKeyLength][hashKey][sortKey] of pegasus records generated by
     * resource data, please make sure the data has been sorted base the above rule, otherwise,
     * don't set false.
     *
     * @param sort true or false, default is "true"
     * @return this
     */
    public AdvancedConfig enableSort(boolean sort) {
      isSort = sort;
      return this;
    }

    public boolean enableDistinct() {
      return isDistinct;
    }

    public boolean enableSort() {
      return isSort;
    }
  }
}
