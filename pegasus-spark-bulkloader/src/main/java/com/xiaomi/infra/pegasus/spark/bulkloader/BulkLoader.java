package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.alibaba.fastjson.JSON;
import com.github.rholder.retry.RetryException;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.pegasus.spark.FDSService;
import com.xiaomi.infra.pegasus.spark.PegasusException;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.Tools;
import com.xiaomi.infra.pegasus.spark.bulkloader.DataMetaInfo.FileInfo;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
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

  private FDSService fdsService;
  private DataWriter dataWriter;

  private Iterator<Tuple2<RocksDBRecord, String>> dataResourceIterator;

  public BulkLoader(
      BulkLoaderConfig config, Iterator<Tuple2<RocksDBRecord, String>> iterator, int partitionId) {

    String dataPathPrefix =
        config.remoteFsUrl
            + config.dataPathRoot
            + "/"
            + config.clusterName
            + "/"
            + config.tableName
            + "/";

    this.dataResourceIterator = iterator;
    this.partitionId = partitionId;

    this.partitionPath = dataPathPrefix + "/" + partitionId + "/";
    this.bulkLoadInfoPath = dataPathPrefix + "/" + BULK_LOAD_INFO;
    this.bulkLoadMetaDataPath = partitionPath + "/" + BULK_LOAD_METADATA;

    this.bulkLoadInfo =
        new BulkLoadInfo(
            config.clusterName, config.tableName, config.tableId, config.tablePartitionCount);
    this.dataMetaInfo = new DataMetaInfo();

    this.fdsService = new FDSService(config);
    this.dataWriter = new DataWriter(new RocksDBOptions(config));
  }

  void start() throws PegasusException {
    try {
      createBulkLoadInfoFile();
      createDataFile(dataResourceIterator);
      createBulkLoadMetaDataFile();
    } catch (Exception e) {
      throw new PegasusException("generated bulkloader data failed, please check and retry!");
    }
  }

  private void createBulkLoadInfoFile() throws PegasusException {
    if (partitionId == 0) {
      try (BufferedWriter bulkLoadInfoWriter = fdsService.getWriter(bulkLoadInfoPath)) {
        bulkLoadInfoWriter.write(JSON.toJSONString(bulkLoadInfo));
        LOG.info("The bulkLoadInfo file is created successful by partition 0.");
      } catch (IOException e) {
        LOG.warn("The bulkLoadInfo file is created failed by partition 0 failed");
        throw new PegasusException("create bulkLoadInfo failed!", e);
      }
    } else {
      LOG.info("The bulkLoadInfo file is created only by partition 0.");
    }
  }

  private void createDataFile(Iterator<Tuple2<RocksDBRecord, String>> iterator)
      throws PegasusException {
    long start = System.currentTimeMillis();
    long count = 0;

    String curSSTFileName = curFileIndex + BULK_DATA_FILE_SUFFIX;
    dataWriter.openWithRetry(partitionPath + curSSTFileName);
    while (iterator.hasNext()) {
      count++;
      RocksDBRecord rocksDBRecord = iterator.next()._1;
      if (curFileSize > SINGLE_FILE_SIZE_THRESHOLD) {
        dataWriter.closeWithRetry();
        LOG.debug(curFileIndex + BULK_DATA_FILE_SUFFIX + " writes complete!");

        curFileIndex++;
        curFileSize = 0L;
        curSSTFileName = curFileIndex + BULK_DATA_FILE_SUFFIX;

        dataWriter.openWithRetry(partitionPath + curSSTFileName);
      }
      curFileSize += dataWriter.writeWithRetry(rocksDBRecord.key(), rocksDBRecord.value());
    }
    if (curFileSize != 0) {
      dataWriter.closeWithRetry();
    } else {
      // dataWriter will be throw exception when closed if dataWriter don't start any kv,
      dataWriter.writeDefaultKV();
      dataWriter.closeWithRetry();
    }

    LOG.info(
        "create sst file time used is "
            + (System.currentTimeMillis() - start)
            + "ms, kv counts = "
            + count
            + " file counts = "
            + curFileIndex);
  }

  private void createBulkLoadMetaDataFile() throws IOException, PegasusException {
    long start = System.currentTimeMillis();
    List<Future> taskList = new ArrayList<>();
    AtomicBoolean isSuccess = new AtomicBoolean(true);

    FileStatus[] fileStatuses = fdsService.getFileStatus(partitionPath);
    for (FileStatus fileStatus : fileStatuses) {
      taskList.add(
          metaInfoCreateTask.submit(
              () -> {
                try {
                  Tools.<Boolean>getDefaultRetryer().call(() -> generateFileMetaInfo(fileStatus));
                } catch (ExecutionException | RetryException e) {
                  isSuccess.set(false);
                  LOG.error("write meta info[" + fileStatus.getPath().toString() + "] failed!");
                }
              }));
    }

    for (Future task : taskList) {
      try {
        task.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new PegasusException("create metaDataInfo failed!", e);
      }
    }

    if (!isSuccess.get()) {
      throw new PegasusException("create metaDataInfo failed!");
    }

    dataMetaInfo.file_total_size = totalSize.get();
    BufferedWriter bulkLoadMetaDataWriter = fdsService.getWriter(bulkLoadMetaDataPath);
    bulkLoadMetaDataWriter.write(JSON.toJSONString(dataMetaInfo));
    bulkLoadMetaDataWriter.close();
    LOG.info("create meta info successfully, time used is " + (System.currentTimeMillis() - start));
  }

  private boolean generateFileMetaInfo(FileStatus fileStatus)
      throws GalaxyFDSClientException, IOException, URISyntaxException {
    String filePath = fileStatus.getPath().toString();

    String fileName = fileStatus.getPath().getName();
    long fileSize = fileStatus.getLen();
    String fileMD5 = fdsService.getFileMD5(filePath);

    FileInfo fileInfo = dataMetaInfo.new FileInfo(fileName, fileSize, fileMD5);
    dataMetaInfo.files.add(fileInfo);

    totalSize.addAndGet(fileSize);

    LOG.debug(fileName + " meta info generates complete!");
    return true;
  }
}
