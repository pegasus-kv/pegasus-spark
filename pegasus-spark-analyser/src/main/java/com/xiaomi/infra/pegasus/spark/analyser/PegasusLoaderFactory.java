package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.analyser.Config.DataType;

class PegasusLoaderFactory {

  public static PegasusLoader createDataLoader(Config config) throws PegasusSparkException {
    if (config.dataType == DataType.COLD_BACKUP) {
      return new ColdBackupLoader((ColdBackupConfig) config);
    } else {
      // TODO(jiashuo) will support more data type, such as online data
      throw new PegasusSparkException("now only support cold backup data");
    }
  }
}
