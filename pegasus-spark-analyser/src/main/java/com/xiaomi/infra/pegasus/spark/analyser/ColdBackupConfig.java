package com.xiaomi.infra.pegasus.spark.analyser;

import static com.xiaomi.infra.pegasus.spark.FDSConfig.loadFDSConfig;
import static com.xiaomi.infra.pegasus.spark.HDFSConfig.loadHDFSConfig;

import com.xiaomi.infra.pegasus.spark.CommonConfig;
import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import java.util.Objects;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

/**
 * ColdBackupConfig is used when you manipulate the cold-backup data. <br>
 * <br>
 * A pegasus cold-backup is a couple of well-organized files dumped from a pegasus table.<br>
 * It's a complete snapshot of the moment.
 */
public class ColdBackupConfig extends CommonConfig implements Config {
  private static final DataType dataType = DataType.COLD_BACKUP;

  private static final long MB_UNIT = 1024 * 1024L;

  private static final int DEFAULT_FILE_OPEN_COUNT = 50;
  private static final long DEFAULT_READ_AHEAD_SIZE_MB = 1;
  private static final String DEFAULT_POLICY_NAME = "one_time";
  private static final int DEFAULT_DATA_VERSION = 1;

  private long readAheadSize;
  private int fileOpenCount;
  private String policyName;
  private String coldBackupTime;
  private DataVersion dataVersion = new DataVersion1();

  public ColdBackupConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    super(hdfsConfig, clusterName, tableName);
    setReadOptions(DEFAULT_FILE_OPEN_COUNT, DEFAULT_READ_AHEAD_SIZE_MB);
  }

  public ColdBackupConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    super(fdsConfig, clusterName, tableName);
    setReadOptions(DEFAULT_FILE_OPEN_COUNT, DEFAULT_READ_AHEAD_SIZE_MB);
  }

  public static ColdBackupConfig loadConfig() throws PegasusSparkException, ConfigurationException {
    return loadConfig(RemoteFSType.FDS);
  }

  public static ColdBackupConfig loadConfig(CommonConfig.RemoteFSType remoteFSType)
      throws ConfigurationException, PegasusSparkException {
    XMLConfiguration configuration =
        new XMLConfiguration(
            Objects.requireNonNull(
                ColdBackupConfig.class.getClassLoader().getResource("core-site.xml")));

    long readAheadSize =
        configuration.getLong("pegasus.analyser.readAheadSize", DEFAULT_READ_AHEAD_SIZE_MB);
    int fileOpenCount =
        configuration.getInt("pegasus.analyser.fileMaxOpenCount", DEFAULT_FILE_OPEN_COUNT);
    DataVersion version =
        configuration.getInt("pegasus.analyser.version", DEFAULT_DATA_VERSION) == 1
            ? new DataVersion1()
            : new DataVersion2();
    String policyName = configuration.getString("pegasus.analyser.policy", DEFAULT_POLICY_NAME);
    String coldBackupTime = configuration.getString("pegasus.analyser.timestamp");
    String clusterName = configuration.getString("pegasus.analyser.cluster");
    String tableName = configuration.getString("pegasus.analyser.table");

    ColdBackupConfig coldBackupConfig;
    if (remoteFSType == RemoteFSType.FDS) {
      coldBackupConfig = new ColdBackupConfig(loadFDSConfig(configuration), clusterName, tableName);
    } else if (remoteFSType == RemoteFSType.HDFS) {
      coldBackupConfig =
          new ColdBackupConfig(loadHDFSConfig(configuration), clusterName, tableName);
    } else {
      throw new PegasusSparkException("Only support fds and hdfs");
    }
    return coldBackupConfig
        .setReadOptions(fileOpenCount, readAheadSize)
        .setColdBackupTime(coldBackupTime)
        .setPolicyName(policyName)
        .setDataVersion(version);
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  /**
   * cold backup policy name
   *
   * @param policyName policyName is pegasus server cold backup concept which is set when creating
   *     cold backup, see https://pegasus.apache.org/administration/cold-backup, here default is
   *     "every_day", you may need change it base your pegasus server config
   * @return this
   */
  public ColdBackupConfig setPolicyName(String policyName) {
    this.policyName = policyName;
    return this;
  }

  /**
   * cold backup creating time.
   *
   * @param coldBackupTime creating time of cold backup data, accurate to day level. for example:
   *     2019-09-11, default is null, means choose the latest data
   * @return this
   */
  public ColdBackupConfig setColdBackupTime(String coldBackupTime) {
    this.coldBackupTime = coldBackupTime;
    return this;
  }

  /**
   * pegasus data version
   *
   * @param dataVersion pegasus data has different data versions, default is {@linkplain
   *     DataVersion1}
   * @return this
   */
  // TODO(wutao1): we can support auto detection of the data version.
  public ColdBackupConfig setDataVersion(DataVersion dataVersion) {
    this.dataVersion = dataVersion;
    return this;
  }

  /**
   * @param maxFileOpenCount maxFileOpenCount is rocksdb concept which can control the max file open
   *     count, default is 50. detail see
   *     https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#general-options
   * @param readAheadSize readAheadSize is rocksdb concept which can control the readAheadSize,
   *     default is 1MB, detail see https://github.com/facebook/rocksdb/wiki/Iterator#read-ahead
   */
  public ColdBackupConfig setReadOptions(int maxFileOpenCount, long readAheadSize) {
    this.readAheadSize = readAheadSize * MB_UNIT;
    this.fileOpenCount = maxFileOpenCount;
    return this;
  }

  public long getReadAheadSize() {
    return readAheadSize;
  }

  public int getFileOpenCount() {
    return fileOpenCount;
  }

  public String getPolicyName() {
    return policyName;
  }

  public String getColdBackupTime() {
    return coldBackupTime;
  }

  public DataVersion getDataVersion() {
    return dataVersion;
  }
}
