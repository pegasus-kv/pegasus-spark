package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class HDFSConfig implements Serializable {
  private String url;
  private String port;

  public HDFSConfig(String url, String Port) {
    this.url = url;
    this.port = Port;
  }

  // The default config will use fs.defaultFS value in the core-site.xml
  public HDFSConfig() {
    this.url = "default";
    this.port = "0";
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setPort(String Port) {
    this.port = Port;
  }

  public String getUrl() {
    return url;
  }

  public String getPort() {
    return port;
  }
}
