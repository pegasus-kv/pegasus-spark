package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class HDFSConfig implements Serializable {
  public String remoteFsUrl;
  public String remoteFsPort;

  public HDFSConfig(String remoteFsUrl, String remoteFsPort) {
    this.remoteFsUrl = remoteFsUrl;
    this.remoteFsPort = remoteFsPort;
  }
}
