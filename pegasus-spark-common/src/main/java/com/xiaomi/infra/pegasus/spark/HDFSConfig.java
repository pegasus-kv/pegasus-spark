package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class HDFSConfig implements Serializable {
  private String url;
  private String Port;

  public HDFSConfig(String url, String Port) {
    this.url = url;
    this.Port = Port;
  }

  public static HDFSConfig loadHDFSConfig(String path) throws ConfigurationException {
    return loadHDFSConfig(new XMLConfiguration(path));
  }

  public static HDFSConfig loadHDFSConfig(XMLConfiguration configuration) {
    String url = configuration.getString("fs.hdfs.url");
    String port = configuration.getString("fs.hdfs.port");
    return new HDFSConfig(url, port);
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setPort(String Port) {
    this.Port = Port;
  }

  public String getUrl() {
    return url;
  }

  public String getPort() {
    return Port;
  }
}
