package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.spark.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.RemoteFileSystem;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.bulkloader.DataMetaInfo.FileInfo;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import scala.Tuple2;

public class BulkLoader {
  private static final Log LOG = LogFactory.getLog(BulkLoader.class);

  private static final int SINGLE_FILE_SIZE_THRESHOLD = 64 * 1024 * 1024;
  private static final String BULK_LOAD_INFO = "bulk_load_info";
  private static final String BULK_LOAD_METADATA = "bulk_load_metadata";
  private static final String BULK_DATA_FILE_SUFFIX = ".sst";

  private ExecutorService metaInfoCreateTask = Executors.newFixedThreadPool(10);
  private final AtomicLong totalSize = new AtomicLong();
  private final int partitionId;
  private int curFileIndex = 1;
  private Long curFileSize = 0L;

  private BulkLoadInfo bulkLoadInfo;
  private DataMetaInfo dataMetaInfo;

  private String partitionPath;
  private String bulkLoadInfoPath;
  private String bulkLoadMetaDataPath;

  private RemoteFileSystem remoteFileSystem;
  private DataWriter dataWriter;

  private Iterator<Tuple2<PegasusKey, PegasusValue>> dataResourceIterator;

  public BulkLoader(
      BulkLoaderConfig config, Iterator<Tuple2<PegasusKey, PegasusValue>> iterator, int partitionId)
      throws PegasusSparkException {

    remoteFileSystem = config.getRemoteFileSystem();

    String dataPathPrefix =
        config.getRemoteFileSystemURL()
            + config.getDataPathRoot()
            + "/"
            + config.getClusterName()
            + "/"
            + config.getTableName()
            + "/";

    this.dataResourceIterator = iterator;
    this.partitionId = partitionId;

    this.partitionPath = dataPathPrefix + "/" + partitionId + "/";
    this.bulkLoadInfoPath = dataPathPrefix + "/" + BULK_LOAD_INFO;
    this.bulkLoadMetaDataPath = partitionPath + "/" + BULK_LOAD_METADATA;

    this.bulkLoadInfo =
        new BulkLoadInfo(
            config.getClusterName(),
            config.getTableName(),
            config.getTableId(),
            config.getTablePartitionCount());
    this.dataMetaInfo = new DataMetaInfo();

    this.dataWriter =
        new DataWriter(
            new RocksDBOptions(config.getRemoteFileSystemURL(), config.getRemoteFileSystemPort()));
  }

  void start() throws PegasusSparkException {
    try {
      checkExistAndDelete();
      createBulkLoadInfoFile();
      createDataFile();
      createBulkLoadMetaDataFile();
    } catch (Exception e) {
      throw new PegasusSparkException("generated bulkloader data failed, please check and retry!");
    }
  }

  private void checkExistAndDelete() throws PegasusSparkException {
    if (remoteFileSystem.exist(partitionPath)) {
      LOG.warn("the data" + partitionPath + "has been existed, and will delete it!");
      remoteFileSystem.delete(partitionPath, true);
    }
  }

  private void createBulkLoadInfoFile() throws PegasusSparkException {
    // all partitions share one bulkLoadInfo file, so just one partition create it, otherwise the
    // filesystem may throw exception
    if (partitionId == 0) {
      try (BufferedWriter bulkLoadInfoWriter = remoteFileSystem.getWriter(bulkLoadInfoPath)) {
        bulkLoadInfoWriter.write(bulkLoadInfo.toJsonString());
        LOG.info("The bulkLoadInfo file is created successful by partition 0.");
      } catch (IOException e) {
        throw new PegasusSparkException("create bulkLoadInfo failed!", e);
      }
    } else {
      LOG.info("The bulkLoadInfo file is created only by partition 0.");
    }
  }

  private void createDataFile() throws PegasusSparkException {
    if (!dataResourceIterator.hasNext()) {
      return;
    }

    long start = System.currentTimeMillis();
    long count = 0;

    String curSSTFileName = curFileIndex + BULK_DATA_FILE_SUFFIX;
    dataWriter.openWithRetry(partitionPath + curSSTFileName);
    while (dataResourceIterator.hasNext()) {
      count++;
      Tuple2<PegasusKey, PegasusValue> record = dataResourceIterator.next();
      if (curFileSize > SINGLE_FILE_SIZE_THRESHOLD) {
        dataWriter.closeWithRetry();
        LOG.debug(curFileIndex + BULK_DATA_FILE_SUFFIX + " writes complete!");

        curFileIndex++;
        curFileSize = 0L;
        curSSTFileName = curFileIndex + BULK_DATA_FILE_SUFFIX;

        dataWriter.openWithRetry(partitionPath + curSSTFileName);
      }

      curFileSize += dataWriter.writeWithRetry(record._1.data(), record._2.data());
    }
    dataWriter.closeWithRetry();
    LOG.info(
        "create partition("
            + partitionId
            + ") sst file complete, time used is "
            + (System.currentTimeMillis() - start)
            + "ms, kv counts = "
            + count
            + " file counts = "
            + curFileIndex);
  }

  private void createBulkLoadMetaDataFile()
      throws PegasusSparkException, ExecutionException, InterruptedException, IOException {
    long start = System.currentTimeMillis();
    List<Future> taskList = new ArrayList<>();
    AtomicInteger successCount = new AtomicInteger();

    FileStatus[] fileStatuses = remoteFileSystem.getFileStatus(partitionPath);

    for (FileStatus fileStatus : fileStatuses) {
      taskList.add(
          metaInfoCreateTask.submit(
              () -> {
                try {
                  generateFileMetaInfo(fileStatus);
                  successCount.incrementAndGet();
                } catch (PegasusSparkException e) {
                  LOG.error("generate meta info[" + fileStatus.getPath().toString() + "] failed!");
                }
              }));
    }

    for (Future task : taskList) {
      task.get();
    }

    if (successCount.get() != fileStatuses.length) {
      throw new PegasusSparkException("some file metaInfo generate failed!");
    }

    dataMetaInfo.file_total_size = totalSize.get();
    BufferedWriter bulkLoadMetaDataWriter = remoteFileSystem.getWriter(bulkLoadMetaDataPath);
    bulkLoadMetaDataWriter.write(dataMetaInfo.toJsonString());
    bulkLoadMetaDataWriter.close();
    LOG.info("create meta info successfully, time used is " + (System.currentTimeMillis() - start));
  }

  private void generateFileMetaInfo(FileStatus fileStatus) throws PegasusSparkException {
    String filePath = fileStatus.getPath().toString();

    String fileName = fileStatus.getPath().getName();
    long fileSize = fileStatus.getLen();
    String fileMD5 = remoteFileSystem.getFileMD5(filePath);

    FileInfo fileInfo = dataMetaInfo.new FileInfo(fileName, fileSize, fileMD5);
    dataMetaInfo.files.add(fileInfo);

    totalSize.addAndGet(fileSize);

    LOG.debug(fileName + " meta info generates complete!");
  }
}
