package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.alibaba.fastjson.JSON;
import com.xiaomi.infra.pegasus.spark.FDSException;
import com.xiaomi.infra.pegasus.spark.FDSService;
import com.xiaomi.infra.pegasus.spark.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.bulkloader.DataMetaInfo.FileInfo;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import scala.Tuple2;

public class BulkLoader {

  private static final Log LOG = LogFactory.getLog(BulkLoader.class);

  private static final ExecutorService writeTask = Executors.newFixedThreadPool(10);
  private static final int MAX_RETRY_TIME = 10;

  public static final String BULK_LOAD_INFO = "bulk_load_info";
  private static final String BULK_LOAD_METADATA = "bulk_load_metadata";
  private static final String SST_SUFFIX = ".sst";
  private final int partitionId;

  private int curSSTFileIndex = 1;
  private Long curSSTFileSize = 0L;
  private Map<String, Long> sstFileList = new HashMap<>();

  // TODO drop table will result in name-id relationship changed
  private BulkLoadInfo bulkLoadInfo;
  private DataMetaInfo dataMetaInfo;

  private String bulkFilePrefix;
  private String partitionPath;
  private String bulkLoadInfoPath;
  private String bulkLoadMetaDataPath;

  private FDSService fdsService;
  private SSTWriter sstWriter;

  private Iterator<Tuple2<RocksDBRecord, String>> dataResourceIterator;

  public BulkLoader(
      BulkLoaderConfig config, Iterator<Tuple2<RocksDBRecord, String>> iterator, int partitionId) {
    this.dataResourceIterator = iterator;
    this.partitionId = partitionId;

    this.bulkFilePrefix =
        config.remoteFsUrl
            + config.pathRoot
            + "/"
            + config.clusterName
            + "/"
            + config.tableName
            + "/";
    this.partitionPath = bulkFilePrefix + "/" + partitionId + "/";
    this.bulkLoadInfoPath = bulkFilePrefix + "/" + BULK_LOAD_INFO;
    this.bulkLoadMetaDataPath = partitionPath + "/" + BULK_LOAD_METADATA;

    this.bulkLoadInfo =
        new BulkLoadInfo(
            config.clusterName, config.tableName, config.tableId, config.tablePartitionCount);
    this.dataMetaInfo = new DataMetaInfo();

    this.fdsService = new FDSService(config);
    this.sstWriter = new SSTWriter(new RocksDBOptions(config));
  }

  void write() throws IOException, FDSException {
    // TODO bulkLoadInfoFile will be write multi time in distributed system or multi thread
    createBulkLoadInfoFile();
    createSSTFile(dataResourceIterator);
    createBulkLoadMetaDataFile();
  }

  private void createSSTFile(Iterator<Tuple2<RocksDBRecord, String>> iterator) throws FDSException {
    long start = System.currentTimeMillis();
    long count = 0;

    String curSSTFileName = curSSTFileIndex + SST_SUFFIX;
    sstWriter.open(partitionPath + curSSTFileName);
    while (iterator.hasNext()) {
      count++;
      RocksDBRecord rocksDBRecord = iterator.next()._1;
      if (curSSTFileSize > 64 * 1024 * 1024) {
        sstWriter.close();
        LOG.info(curSSTFileIndex + SST_SUFFIX + " write complete!");

        curSSTFileIndex++;
        curSSTFileSize = 0L;
        curSSTFileName = curSSTFileIndex + SST_SUFFIX;

        sstWriter.open(partitionPath + curSSTFileName);
      }
      curSSTFileSize += sstWriter.write(rocksDBRecord.key(), rocksDBRecord.value());
    }
    if (curSSTFileSize != 0) {
      sstWriter.close();
    } else {
      // sstWriter will be throw exception when closed if sstWriter don't write any kv,
      sstWriter.writeNullKV();
      sstWriter.close();
    }

    LOG.info(
        "create sst file time used is "
            + (System.currentTimeMillis() - start)
            + "ms, kv count = "
            + count
            + "file count = "
            + curSSTFileIndex);
  }

  private void createBulkLoadInfoFile() throws FDSException {
    if (partitionId == 0) {
      boolean isSuccess = false;
      int count = 0;

      while (!isSuccess && count++ <= MAX_RETRY_TIME) {
        try (BufferedWriter bulkLoadInfoWriter = fdsService.getWriter(bulkLoadInfoPath)) {
          bulkLoadInfoWriter.write(JSON.toJSONString(bulkLoadInfo));
          LOG.info("The bulkLoadInfo file is created successful by partition 0.");
          isSuccess = true;
        } catch (Exception e) {
          LOG.info("The bulkLoadInfo file is created failed by partition 0, retry = " + count, e);
          if (count > MAX_RETRY_TIME) {
            throw new FDSException("create bulkLoadInfo failed!");
          }
        }
      }
    } else {
      LOG.info("The bulkLoadInfo file is only created one time by partition 0.");
    }
  }

  private void createBulkLoadMetaDataFile() throws IOException, FDSException {
    long start = System.currentTimeMillis();
    List<Future> taskList = new ArrayList<>();

    final AtomicLong totalSize = new AtomicLong();

    FileStatus[] fileStatuses = fdsService.getFileStatus(partitionPath);
    for (FileStatus fileStatus : fileStatuses) {
      taskList.add(
          writeTask.submit(
              () -> {
                boolean isSuccess = false;
                int count = 0;

                while (!isSuccess && count++ <= MAX_RETRY_TIME) {
                  try {

                    String filePath = fileStatus.getPath().toString();

                    String fileName = fileStatus.getPath().getName();
                    long fileSize = fileStatus.getLen();
                    String fileMD5 = fdsService.getFileMD5(filePath);

                    FileInfo fileInfo = dataMetaInfo.new FileInfo(fileName, fileSize, fileMD5);
                    dataMetaInfo.files.add(fileInfo);

                    totalSize.addAndGet(fileSize);

                    LOG.info(fileName + " meta info complete!");
                    isSuccess = true;

                  } catch (Exception e) {
                    LOG.warn(
                        "create file meta info failed, file = ["
                            + fileStatus.getPath().toString()
                            + "], retry = "
                            + count,
                        e);
                  }
                }
              }));
    }

    for (Future task : taskList) {
      try {
        task.get();
      } catch (InterruptedException | ExecutionException e) {
        LOG.warn("future get error!");
        throw new FDSException("create metaDataInfo failed!", e);
      }
    }

    dataMetaInfo.file_total_size = totalSize.get();

    boolean isSuccess = false;
    int count = 0;

    while (!isSuccess && count++ <= MAX_RETRY_TIME) {
      try (BufferedWriter bulkLoadMetaDataWriter = fdsService.getWriter(bulkLoadMetaDataPath)) {
        bulkLoadMetaDataWriter.write(JSON.toJSONString(dataMetaInfo));
        isSuccess = true;
      } catch (Exception e) {
        LOG.warn("write meta info failed, retry = " + count, e);
        if (count > MAX_RETRY_TIME) {
          throw new FDSException("create meta info failed!");
        }
      }
    }
    LOG.info("create meta info time used is " + (System.currentTimeMillis() - start));
  }
}
