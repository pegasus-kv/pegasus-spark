package com.xiaomi.infra.pegasus.analyser;

public class FDSException extends Exception {
  private static final String versionPrefix = loadVersion() + ": ";

  public FDSException(String message, Throwable cause) {
    super(versionPrefix + message, cause);
  }

  public FDSException(String message) {
    super(versionPrefix + message);
  }

  public FDSException(Throwable cause) {
    super(versionPrefix + cause.toString(), cause);
  }

  private static String loadVersion() {
    String ver = FDSException.class.getPackage().getImplementationVersion();
    if (ver == null) {
      return "{version}";
    }
    return ver;
  }
}
