package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.Config;

public class BulkLoaderConfig extends Config {

  public boolean isDistinct = false;
  public boolean isSort = true;
  public String pathRoot = "/temp/bulkLoader";

  public BulkLoaderConfig setDistinct(boolean distinct) {
    isDistinct = distinct;
    return this;
  }

  public BulkLoaderConfig setSort(boolean sort) {
    isSort = sort;
    return this;
  }

  public BulkLoaderConfig setPathRoot(String pathRoot) {
    this.pathRoot = pathRoot;
    return this;
  }
}
