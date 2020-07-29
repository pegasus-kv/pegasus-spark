package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.github.rholder.retry.RetryException;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.Tools;
import java.util.concurrent.ExecutionException;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

/** The wrapper of sstFileWriter of rocksdbjava */
class DataWriter {

  private SstFileWriter sstFileWriter;

  DataWriter(RocksDBOptions rocksDBOptions) {
    this.sstFileWriter = new SstFileWriter(rocksDBOptions.envOptions, rocksDBOptions.options);
  }

  void openWithRetry(String path) throws PegasusSparkException {
    try {
      Tools.<Boolean>getDefaultRetryer().call(() -> open(path));
    } catch (ExecutionException | RetryException e) {
      throw new PegasusSparkException("sstFileWriter open [" + path + "] failed!", e);
    }
  }

  private boolean open(String path) throws RocksDBException {
    sstFileWriter.open(path);
    return true;
  }

  int writeWithRetry(byte[] key, byte[] value) throws PegasusSparkException {
    try {
      return Tools.<Integer>getDefaultRetryer().call(() -> write(key, value));
    } catch (ExecutionException | RetryException e) {
      throw new PegasusSparkException(
          "sstFileWriter put key-value[key=" + new String(key) + "] failed!", e);
    }
  }

  private int write(byte[] key, byte[] value) throws RocksDBException {
    sstFileWriter.put(key, value);
    return key.length + value.length;
  }

  void closeWithRetry() throws PegasusSparkException {
    try {
      Tools.<Boolean>getDefaultRetryer().call(this::close);
    } catch (ExecutionException | RetryException e) {
      throw new PegasusSparkException("sstFileWriter close failed!", e);
    }
  }

  public boolean close() throws RocksDBException {
    sstFileWriter.finish();
    return true;
  }
}
