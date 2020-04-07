package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.Config;

public class ColdBackupConfig extends Config {

  public String policyName = "every_day";
  // creating time of cold backup data, for example: 2019-09-11
  public String coldBackupTime = "";
  // different kv store has different restore version, default set "RecordRestoreV1"
  public RecordRestore recordRestore = new RecordRestoreV1();

  public Config setPolicyName(String policyName) {
    this.policyName = policyName;
    return this;
  }

  public Config setColdBackupTime(String coldBackupTime) {
    this.coldBackupTime = coldBackupTime;
    return this;
  }

  public Config setRecordRestore(RecordRestore recordRestore) {
    this.recordRestore = recordRestore;
    return this;
  }
}
