package com.xiaomi.infra.pegasus.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Serializable;
import org.apache.hadoop.fs.FileStatus;

public interface RemoteFileSystem extends Serializable {

  public BufferedReader getReader(String filePath) throws PegasusSparkException;

  public BufferedWriter getWriter(String filePath) throws PegasusSparkException;

  public FileStatus[] getFileStatus(String path) throws PegasusSparkException;

  public String getFileMD5(String filePath) throws PegasusSparkException;
}
