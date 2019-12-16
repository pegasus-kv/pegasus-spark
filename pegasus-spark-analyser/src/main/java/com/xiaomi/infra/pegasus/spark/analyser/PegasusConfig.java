package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.Config;

public class PegasusConfig extends Config {

  public String policyName = "every_day";
  public String coldDataTime = "";

  public Config setPolicyName(String policyName) {
    this.policyName = policyName;
    return this;
  }

  public Config setColdDataTime(String coldDataTime) {
    this.coldDataTime = coldDataTime;
    return this;
  }
}
