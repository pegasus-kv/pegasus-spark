package com.xiaomi.infra.pegasus.spark.analyser;

public interface PegasusScanner {

  public boolean isValid();

  public void seekToFirst();

  public void next();

  public void close();

  public PegasusRecord restore();
}
