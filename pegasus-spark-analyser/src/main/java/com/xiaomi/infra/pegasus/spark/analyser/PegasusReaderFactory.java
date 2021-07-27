package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.spark.PegasusSparkException;

class PegasusReaderFactory {

  public static PegasusReader createDataReader(Config cfg)
      throws PegasusSparkException, PException {
    switch (cfg.getDataType()) {
      case COLD_BACKUP:
        return new ColdBackupReader((ColdBackupConfig) cfg);
      case ONLINE_DATA:
        return new OnlineDataReader((OnlineDataConfig) cfg);
      default:
        throw new PegasusSparkException(
            "now only support cold backup data and online data, data type = " + cfg.getDataType());
    }
  }
}
