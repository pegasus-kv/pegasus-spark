package com.xiaomi.infra.pegasus.spark.bulkloader;

import java.util.ArrayList;

public class DataMetaInfo {

  ArrayList<FileInfo> files = new ArrayList<>();
  long file_total_size;

  class FileInfo {

    String name;
    long size;
    String md5;

    public FileInfo(String name, long size, String md5) {
      this.name = name;
      this.size = size;
      this.md5 = md5;
    }
  }
}
