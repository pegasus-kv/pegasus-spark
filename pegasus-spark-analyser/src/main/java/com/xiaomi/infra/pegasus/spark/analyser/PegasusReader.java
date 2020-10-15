package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import java.io.Serializable;
import org.rocksdb.RocksDBException;

public interface PegasusReader extends Serializable {

  int getPartitionCount() throws PException;

  Config getConfig();

  PegasusScanner getScanner(int pid) throws PegasusSparkException, RocksDBException, PException;
}
