package com.xiaomi.infra.pegasus.spark.analyser;

import java.io.Serializable;

public interface Config extends Serializable {

  enum DataType {
    COLD_BACKUP,
    ONLINE_DATA,
    INVALID
  }

  DataType getDataType();

  String getClusterName();

  String getTableName();
}
