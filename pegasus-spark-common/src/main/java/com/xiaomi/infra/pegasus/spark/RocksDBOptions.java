package com.xiaomi.infra.pegasus.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;

public class RocksDBOptions implements AutoCloseable {
  private static final Log LOG = LogFactory.getLog(RocksDBOptions.class);

  public Options options;
  public ReadOptions readOptions;
  public EnvOptions envOptions;
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
            .setMaxOpenFiles(config.DBMaxFileOpenCounter);

    readOptions = new ReadOptions().setReadaheadSize(config.DBReadAheadSize);

    Logger rocksDBLog =
        new Logger(options) {
          @Override
          public void log(InfoLogLevel infoLogLevel, String s) {
            LOG.info("[rocksDB native log info]" + s);
          }
        };

    options.setLogger(rocksDBLog);
    envOptions = new EnvOptions().setWritableFileMaxBufferSize(1000L);
  }

  @Override
  public void close() {
    options.close();
    readOptions.close();
    env.close();
  }
}
