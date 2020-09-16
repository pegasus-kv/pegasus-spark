package com.xiaomi.infra.pegasus.spark.bulkloader;

import static com.xiaomi.infra.pegasus.spark.FDSConfig.loadFDSConfig;
import static com.xiaomi.infra.pegasus.spark.HDFSConfig.loadHDFSConfig;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

/**
 * The config used for generating the pegasus data which will be placed as follow":
 *
 * <p><DataPathRoot>/<ClusterName>/<TableName>
 * <DataPathRoot>/<ClusterName>/<TableName>/bulk_load_info => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/bulk_load_metadata => {JSON}
 * <DataPathRoot>/<ClusterName>/<TableName>/<PartitionIndex>/<FileIndex>.sst => RocksDB SST File
 */
public class BulkLoaderConfig extends CommonConfig {
  public static final String DEFAULT_DATA_PATH_ROOT = "/pegasus-bulkloader";

  private AdvancedConfig advancedConfig;

  private String dataPathRoot;
  private int tableId;
  private int tablePartitionCount;

  public BulkLoaderConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    super(hdfsConfig, clusterName, tableName);
    initConfig();
  }

  public BulkLoaderConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
    initConfig();
  }

  private void initConfig() {
    this.dataPathRoot = DEFAULT_DATA_PATH_ROOT;
    this.advancedConfig = new AdvancedConfig();
  }

  public static BulkLoaderConfig loadConfig() throws PegasusSparkException, ConfigurationException {
    return loadConfig(RemoteFSType.FDS);
  }

  public static BulkLoaderConfig loadConfig(RemoteFSType remoteFSType)
      throws ConfigurationException, PegasusSparkException {
    XMLConfiguration configuration =
        new XMLConfiguration(
            Objects.requireNonNull(
                    BulkLoaderConfig.class.getClassLoader().getResource("core-site.xml"))
                .getPath());
    String clusterName = configuration.getString("pegasus.bulkloader.cluster");
    String tableName = configuration.getString("pegasus.bulkloader.table");
    int tableId = configuration.getInt("pegasus.bulkloader.id");
    int tablePartitionCount = configuration.getInt("pegasus.bulkloader.count");
    String dataPathRoot =
        configuration.getString("pegasus.bulkloader.root", DEFAULT_DATA_PATH_ROOT);
    boolean enableSort =
        configuration.getBoolean("pegasus.bulkloader.sort", AdvancedConfig.DEFAULT_ENABLE_SORT);
    boolean enableDistinct =
        configuration.getBoolean(
            "pegasus.bulkloader.distinct", AdvancedConfig.DEFAULT_ENABLE_DISTINCT);

    BulkLoaderConfig bulkLoaderConfig;
    if (remoteFSType == RemoteFSType.FDS) {
      bulkLoaderConfig = new BulkLoaderConfig(loadFDSConfig(configuration), clusterName, tableName);
    } else if (remoteFSType == RemoteFSType.HDFS) {
      bulkLoaderConfig =
          new BulkLoaderConfig(loadHDFSConfig(configuration), clusterName, tableName);
    } else {
      throw new PegasusSparkException("Only support fds and hdfs");
    }

    return bulkLoaderConfig
        .setAdvancedConfig(
            new AdvancedConfig().enableDistinct(enableDistinct).enableSort(enableSort))
        .setDataPathRoot(dataPathRoot)
        .setTableId(tableId)
        .setTablePartitionCount(tablePartitionCount);
  }

  /**
   * pegasus table ID
   *
   * <p>TODO(jiashuo): support automatically retrieval of the table ID of the specified table name
   *
   * @param tableId
   * @return this
   */
  public BulkLoaderConfig setTableId(int tableId) {
    this.tableId = tableId;
    return this;
  }

  /**
   * pegasus table partition count
   *
   * <p>TODO(jiashuo): support automatically retrieval of the partition count of the specified table
   * name
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
    public static final boolean DEFAULT_ENABLE_SORT = true;
    public static final boolean DEFAULT_ENABLE_DISTINCT = true;

    private boolean isDistinct;
    private boolean isSort;

    public AdvancedConfig() {
      this.isDistinct = DEFAULT_ENABLE_DISTINCT;
      this.isSort = DEFAULT_ENABLE_SORT;
    }

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
