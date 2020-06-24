package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.FDSConfig;
import com.xiaomi.infra.pegasus.spark.FDSFileSystem;
import com.xiaomi.infra.pegasus.spark.HDFSConfig;
import com.xiaomi.infra.pegasus.spark.HDFSFileSystem;
import com.xiaomi.infra.pegasus.spark.RemoteFileSystem;
import java.io.Serializable;

public class ColdBackupConfig implements Serializable {
  private static final long MB_UNIT = 1024 * 1024L;

  private static final int DEFAULT_FILE_OPEN_COUNT = 50;
  private static final long DEFAULT_READ_AHEAD_SIZE = 1 * MB_UNIT;

  public String remoteFileSystemURL;
  public String remoteFileSystemPort;

  public long readAheadSize;
  public int fileOpenCount;

  public String clusterName;
  public String tableName;
  public String policyName;
  public String coldBackupTime;

  public DataVersion dataVersion = new DataVersion1();

  public RemoteFileSystem remoteFileSystem;

  public ColdBackupConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    initConfig(hdfsConfig, clusterName, tableName);
    this.remoteFileSystem = new HDFSFileSystem();
  }

  public ColdBackupConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    initConfig(fdsConfig, clusterName, tableName);
    this.remoteFileSystem = new FDSFileSystem(fdsConfig);
  }

  private void initConfig(HDFSConfig config, String clusterName, String tableName) {
    this.remoteFileSystemURL = config.remoteFsUrl;
    this.remoteFileSystemPort = config.remoteFsPort;
    this.clusterName = clusterName;
    this.tableName = tableName;
    setReadOptions(DEFAULT_FILE_OPEN_COUNT, DEFAULT_READ_AHEAD_SIZE);
  }

  /**
   * cold backup policy name
   *
   * @param policyName policyName is pegasus server cold backup concept which is set when creating
   *     cold backup, see https://pegasus-kv.github.io/administration/cold-backup, here default is
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
   *     2019-09-11, default is "", means choose the latest data
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
  public void setReadOptions(int maxFileOpenCount, long readAheadSize) {
    this.readAheadSize = readAheadSize * MB_UNIT;
    this.fileOpenCount = maxFileOpenCount;
  }
}
