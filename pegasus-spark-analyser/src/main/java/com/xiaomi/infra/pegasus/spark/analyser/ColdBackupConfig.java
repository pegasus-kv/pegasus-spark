package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.Config;

public class ColdBackupConfig extends Config {

  public String policyName = "every_day";
  // creating time of cold backup data, for example: 2019-09-11
  public String coldBackupTime = "";
  // pegasus has different data versions, default is "DataVersion1"
  public DataVersion dataVersion = new DataVersion1();

  public Config setPolicyName(String policyName) {
    this.policyName = policyName;
    return this;
  }

  public Config setColdBackupTime(String coldBackupTime) {
    this.coldBackupTime = coldBackupTime;
    return this;
  }

  // TODO(wutao1): we can support auto detection of the data version.
  public Config setDataVersion(DataVersion dataVersion) {
    this.dataVersion = dataVersion;
    return this;
  }
}
