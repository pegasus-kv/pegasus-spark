package com.xiaomi.infra.pegasus.spark.bulkloader;

import java.util.ArrayList;

public class DataMetaInfo {

  public ArrayList<FileInfo> files = new ArrayList<>();
  public long file_total_size;

  public class FileInfo {

    public FileInfo(String name, long size, String md5) {
      this.name = name;
      this.size = size;
      this.md5 = md5;
    }

    public String name;
    public long size;
    public String md5;
  }
}
