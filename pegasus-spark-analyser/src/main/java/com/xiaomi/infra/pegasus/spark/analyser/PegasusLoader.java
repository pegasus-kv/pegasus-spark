package com.xiaomi.infra.pegasus.spark.analyser;

import java.io.Serializable;
import java.util.Map;

public interface PegasusLoader extends Serializable {

  int getPartitionCount();

  Map<Integer, String> getCheckpointUrls();
}
