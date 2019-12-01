package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.core.Tools;
import com.xiaomi.infra.pegasus.spark.core.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.core.RocksDBRecord;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import java.util.Stack;

public class PegasusService implements AutoCloseable {

  SstFileWriter sstFileWriter;
  int partitionCount;
  int partitionId;
  RocksDBOptions rocksDBOptions;

  public PegasusService(RocksDBOptions rocksDBOptions, int partitionCount, int partitionId) {
    this.rocksDBOptions = rocksDBOptions;
    this.partitionCount = partitionCount;
    this.partitionId = partitionId;
    this.sstFileWriter = new SstFileWriter(rocksDBOptions.envOptions, rocksDBOptions.options);
  }

  public int write(byte[] key, byte[] value) throws RocksDBException {
    if (isCurrentPid(key)) {
      try {
        sstFileWriter.put(key, value);
      } catch (RocksDBException e) {
        throw new RocksDBException(e.getMessage());
      }
      return key.length + value.length;
    }
    return 0;
  }

  void writeNoHashCheck(String sortKey, String value)
      throws RocksDBException {
    byte[] pegasusKey = RocksDBRecord.generateKey("null".getBytes(), sortKey.getBytes());
    byte[] pegasusValue =RocksDBRecord.generateValue(value.getBytes());
    // todo spark实际可以先根据自定义的hash算法分配，这里默认每个节点读取所有的数据，手动分片，这其实是不合理的。
    sstFileWriter.put(pegasusKey, pegasusValue);
  }

  void open(String path) throws RocksDBException {
    sstFileWriter.open(path);
  }

  @Override
  public void close() throws RocksDBException {
    sstFileWriter.finish();
  }

  private boolean isCurrentPid(byte[] pegasusKey) {
    return Tools.remainderUnsigned(Tools.hash(pegasusKey), partitionCount) == partitionId;
  }

}
