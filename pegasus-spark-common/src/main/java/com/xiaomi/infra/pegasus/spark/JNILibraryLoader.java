package com.xiaomi.infra.pegasus.spark;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.RocksDB;

public class JNILibraryLoader {
  static class JNILibraries {
    List<String> libraries = new ArrayList<>();

    JNILibraries add(String library) {
      libraries.add(library);
      return this;
    }
  }

  private static final String libraryPrefix = "/lib/";
  private static final String LIB_BZ2 = "libbz2.so.1.0";
  private static final String LIB_HDFS = "libhdfs.so.0.0.0";
  private static final String LIB_JAVA = "libjava.so";
  private static final String LIB_JVM = "libjvm.so";
  private static final String LIB_LZ4 = "liblz4.so.1";
  private static final String LIB_SNAPPY = "libsnappy.so.1";
  private static final String LIB_STDCPLUS = "libstdc++.so.6";
  private static final String LIB_VERIFY = "libverify.so";
  private static final String LIB_Z = "libz.so.1";
  private static final String LIB_ZSTD = "libzstd.so.0";

  private static volatile File temporaryDir;
  private static JNILibraries jniLibraries = new JNILibraries();

  static {
    jniLibraries
        .add(LIB_BZ2)
        .add(LIB_HDFS)
        .add(LIB_JAVA)
        .add(LIB_JVM)
        .add(LIB_LZ4)
        .add(LIB_SNAPPY)
        .add(LIB_STDCPLUS)
        .add(LIB_VERIFY)
        .add(LIB_Z)
        .add(LIB_ZSTD);
  }

  public static void load() throws IOException, PegasusSparkException {
    for (String lib : jniLibraries.libraries) {
      load(lib);
    }
    RocksDB.loadLibrary();
  }

  private static void load(String libraryName) throws IOException, PegasusSparkException {
    String libraryPath = libraryPrefix + libraryName;

    if (temporaryDir == null) {
      synchronized (JNILibraryLoader.class) {
        if (temporaryDir == null) {
          temporaryDir = generateTempFile();
          temporaryDir.deleteOnExit();
        }
      }
    }

    File temp = new File(temporaryDir, libraryName);
    if (!temp.exists()) {
      synchronized (JNILibraryLoader.class) {
        if (!temp.exists()) {
          InputStream in = JNILibraryLoader.class.getResourceAsStream(libraryPath);
          if (in == null) {
            throw new PegasusSparkException(libraryPath + " not found!");
          }
          Files.copy(in, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
          System.load(temp.getAbsolutePath());
        }
      }
    }
  }

  private static File generateTempFile() throws IOException {
    String tempDir = System.getProperty("java.io.tmpdir");
    File generatedDir = new File(tempDir, "pegasus-spark" + System.nanoTime());

    if (!generatedDir.mkdir())
      throw new IOException("Failed to create temp directory " + generatedDir.getName());
    return generatedDir;
  }
}
