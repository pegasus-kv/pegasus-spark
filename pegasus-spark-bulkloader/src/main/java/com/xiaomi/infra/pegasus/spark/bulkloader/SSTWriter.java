package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.FDSException;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.SstFileWriter;

public class SSTWriter implements AutoCloseable {

  private static final Log LOG = LogFactory.getLog(SSTWriter.class);

  private static final int MAX_RETRY_TIME = 10;
  private SstFileWriter sstFileWriter;

  public SSTWriter(RocksDBOptions rocksDBOptions) {
    this.sstFileWriter = new SstFileWriter(rocksDBOptions.envOptions, rocksDBOptions.options);
  }

  public int write(byte[] key, byte[] value) throws FDSException {
    boolean isSuccess = false;
    int count = 0;

    while (!isSuccess && count++ <= MAX_RETRY_TIME) {
      try {
        sstFileWriter.put(key, value);
        isSuccess = true;
      } catch (Exception e) {
        LOG.warn("sstFileWriter put kv failed, retry = " + count, e);
        if (count > MAX_RETRY_TIME) {
          throw new FDSException("create meta info failed!");
        }
      }
    }
    return key.length + value.length;
  }

  /** only for writing "null-null" kv, see {@link BulkLoader} createSSTFile method */
  void writeNullKV() throws FDSException {
    byte[] pegasusKey = RocksDBRecord.generateKey("null".getBytes(), "null".getBytes());
    byte[] pegasusValue = RocksDBRecord.generateValue("null".getBytes());
    write(pegasusKey, pegasusValue);
  }

  void open(String path) throws FDSException {
    boolean isSuccess = false;
    int count = 0;

    while (!isSuccess && count++ <= MAX_RETRY_TIME) {
      try {
        sstFileWriter.open(path);
        isSuccess = true;
      } catch (Exception e) {
        LOG.warn("sstFileWriter open [" + path + "] failed, retry = " + count, e);
        if (count > MAX_RETRY_TIME) {
          throw new FDSException("create meta info failed!");
        }
      }
    }
  }

  @Override
  public void close() throws FDSException {
    boolean isSuccess = false;
    int count = 0;

    while (!isSuccess && count++ <= MAX_RETRY_TIME) {
      try {
        sstFileWriter.finish();
        isSuccess = true;
      } catch (Exception e) {
        LOG.warn("sstFileWriter flush failed, retry = " + count, e);
        if (count > MAX_RETRY_TIME) {
          throw new FDSException("create meta info failed!");
        }
      }
    }
  }
}
