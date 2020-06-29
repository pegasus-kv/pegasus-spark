package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.github.rholder.retry.RetryException;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.Tools;
import java.util.concurrent.ExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

/** The wrapper of sstFileWriter of rocksdbjava */
public class DataWriter {

  private static final Log LOG = LogFactory.getLog(DataWriter.class);

  // TODO: The two variables only for partitions which contain nothing, set the ttl = 10s is for
  // voiding being duplication with real kv when load to tables.
  private static final byte[] PEGASUS_NULL_KEY =
      PegasusRecord.generateKey("NULL".getBytes(), "NULL".getBytes());
  private static final byte[] PEGASUS_NULL_VALUE =
      PegasusRecord.generateValue("NULL".getBytes(), 10);

  private SstFileWriter sstFileWriter;

  public DataWriter(RocksDBOptions rocksDBOptions) {
    this.sstFileWriter = new SstFileWriter(rocksDBOptions.envOptions, rocksDBOptions.options);
  }

  public int writeWithRetry(byte[] key, byte[] value) throws PegasusSparkException {
    try {
      return Tools.<Integer>getDefaultRetryer().call(() -> write(key, value));
    } catch (ExecutionException | RetryException e) {
      LOG.warn("sstFileWriter put key-value[key=" + new String(key) + "] failed!");
      throw new PegasusSparkException(
          "sstFileWriter put key-value[key=" + new String(key) + "] failed!", e);
    }
  }

  public int write(byte[] key, byte[] value) throws RocksDBException {
    sstFileWriter.put(key, value);
    return key.length + value.length;
  }

  /** only for writing "NULL-NULL" kv, see {@link BulkLoader} createDataFile method */
  public void writeDefaultKV() throws PegasusSparkException {
    writeWithRetry(PEGASUS_NULL_KEY, PEGASUS_NULL_VALUE);
  }

  public void openWithRetry(String path) throws PegasusSparkException {
    try {
      Tools.<Boolean>getDefaultRetryer().call(() -> open(path));
    } catch (ExecutionException | RetryException e) {
      LOG.warn("sstFileWriter open [" + path + "] failed!");
      throw new PegasusSparkException("sstFileWriter open [" + path + "] failed!", e);
    }
  }

  public boolean open(String path) throws RocksDBException {
    sstFileWriter.open(path);
    return true;
  }

  public void closeWithRetry() throws PegasusSparkException {
    try {
      Tools.<Boolean>getDefaultRetryer().call(this::close);
    } catch (ExecutionException | RetryException e) {
      LOG.warn("sstFileWriter close failed!");
      throw new PegasusSparkException("sstFileWriter close failed!", e);
    }
  }

  public boolean close() throws RocksDBException {
    sstFileWriter.finish();
    return true;
  }
}
