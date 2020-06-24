package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.FSConfig;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;

public class ColdBackupConfig extends FSConfig {

  private static final long MB_UNIT = 1024 * 1024L;

  public String policyName = "every_day";
  public String coldBackupTime = "";
  public String clusterName;
  public String tableName;

  public DataVersion dataVersion = new DataVersion1();
  public int maxFileOpenCount = 50;
  public long readAheadSize = 1 * MB_UNIT;

  /**
   * the constructor can be used for fds and hdfs
   *
   * @param remoteFsUrl
   * @param remoteFsPort
   * @throws PegasusSparkException
   */
  public ColdBackupConfig(String remoteFsUrl, String remoteFsPort) throws PegasusSparkException {
    super(remoteFsUrl, remoteFsPort);
    setMaxFileOpenCount(maxFileOpenCount);
    setReadAheadSize(readAheadSize);
  }

  /**
   * the contructor only be used for fds
   *
   * @param accessKey
   * @param accessSecret
   * @param bucketName
   * @param endPoint
   * @param port
   * @throws PegasusSparkException
   */
  public ColdBackupConfig(
      String accessKey, String accessSecret, String bucketName, String endPoint, String port)
      throws PegasusSparkException {
    super(accessKey, accessSecret, bucketName, endPoint, port);
    setMaxFileOpenCount(maxFileOpenCount);
    setReadAheadSize(readAheadSize);
  }

  /**
   * the cluster name and table name of target data
   *
   * @param clusterName
   * @param tableName
   * @return
   */
  public ColdBackupConfig setTableInfo(String clusterName, String tableName) {
    this.clusterName = clusterName;
    this.tableName = tableName;
    return this;
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
   * the max file open count
   *
   * @param maxFileOpenCount maxFileOpenCount is rocksdb concept which can control the max file open
   *     count, default is 50. detail see
   *     https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#general-options
   * @return this
   */
  public ColdBackupConfig setMaxFileOpenCount(int maxFileOpenCount) {
    this.maxFileOpenCount = maxFileOpenCount;
    rocksDBOptions.options.setMaxOpenFiles(maxFileOpenCount);
    return this;
  }

  /**
   * readAhead size
   *
   * @param readAheadSize readAheadSize is rocksdb concept which can control the readAheadSize,
   *     default is 1MB, detail see https://github.com/facebook/rocksdb/wiki/Iterator#read-ahead
   * @return this
   */
  public ColdBackupConfig setReadAheadSize(long readAheadSize) {
    this.readAheadSize = readAheadSize;
    rocksDBOptions.readOptions.setReadaheadSize(this.readAheadSize * MB_UNIT);
    return this;
  }
}
