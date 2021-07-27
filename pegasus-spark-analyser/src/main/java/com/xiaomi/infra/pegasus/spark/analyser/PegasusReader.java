package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import java.io.Serializable;

public interface PegasusReader extends Serializable {

  Config getConfig();

  int getPartitionCount();

  PegasusScanner getScanner(int pid) throws PegasusSparkException;
}
