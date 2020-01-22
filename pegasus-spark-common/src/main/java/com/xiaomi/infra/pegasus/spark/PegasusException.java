package com.xiaomi.infra.pegasus.spark;

public class PegasusException extends Exception {
  private static final String VERSION_PREFIX = loadVersion() + ": ";

  public PegasusException(String message, Throwable cause) {
    super(VERSION_PREFIX + message, cause);
  }

  public PegasusException(String message) {
    super(VERSION_PREFIX + message);
  }

  public PegasusException(Throwable cause) {
    super(VERSION_PREFIX + cause.toString(), cause);
  }

  private static String loadVersion() {
    String ver = PegasusException.class.getPackage().getImplementationVersion();
    if (ver == null) {
      return "{version}";
    }
    return ver;
  }
}
