package com.xiaomi.infra.pegasus.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;

/**
 * The wrapper of RocksDB Options in JNI.
 *
 * <p>NOTE: Must be closed manually to release the underlying memory.
 */
public class RocksDBOptions {

  private static final Log LOG = LogFactory.getLog(RocksDBOptions.class);

  public Options options;
  public ReadOptions readOptions;
  private Env env;

  public RocksDBOptions(Config config) {
    if (config.destinationUrl.contains("fds")) {
      env = new HdfsEnv(config.destinationUrl + "#" + config.destinationPort);
    } else {
      env = new HdfsEnv(config.destinationUrl + ":" + config.destinationPort);
    }

    options =
        new Options()
            .setDisableAutoCompactions(true)
            .setCreateIfMissing(true)
            .setEnv(env)
            .setLevel0FileNumCompactionTrigger(-1)
            .setMaxOpenFiles(config.dbMaxFileOpenCounter);

    readOptions = new ReadOptions().setReadaheadSize(config.dbReadAheadSize);

    Logger rocksDBLog =
        new Logger(options) {
          @Override
          public void log(InfoLogLevel infoLogLevel, String s) {
            LOG.info("[rocksDB native log info]" + s);
          }
        };

    options.setLogger(rocksDBLog);
  }

  public void close() {
    options.close();
    readOptions.close();
    env.close();
  }
}
