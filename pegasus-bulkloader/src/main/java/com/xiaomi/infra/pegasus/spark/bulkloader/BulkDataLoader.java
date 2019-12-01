package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.alibaba.fastjson.JSON;
import com.xiaomi.infra.pegasus.spark.bulkloader.DataMetaInfo.FileInfo;
import com.xiaomi.infra.pegasus.spark.core.*;
import org.apache.hadoop.fs.FileStatus;
import org.rocksdb.RocksDBException;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BulkDataLoader {

    public static final String BULK_LOAD_INFO = "bulk_load_info";
    public static final String BULK_LOAD_METADATA = "bulk_load_metadata";
    public static final String BULK_LOAD_DATA_ROOT = "/tmp/pegasus_bulkload_data";
    public static final String SST_SUFFIX = ".sst";


    private int curSSTFileIndex = 1;
    private Long curSSTFileSize = 0L;
    // TODO 这个是不是可以做最后的size的数据，而不用使用文件系统读取文件的size？疑问点主要是考虑计算的方式是否准确
    private Map<String, Long> sstFileList = new HashMap<>();

    // TODO drop table 将导致关系不是一一对应
    public BulkLoadInfo bulkLoadInfo;
    public DataMetaInfo dataMetaInfo;

    public int partitionCount;
    public int partitionId;

    public String bulkFilePrefix;
    public String partitionPath;
    public String bulkLoadInfoPath;
    public String bulkLoadMetaDataPath;

    public FDSService fdsService;
    public SSTWriter sstWriter;

    Iterator<Tuple2<RocksDBRecord, String>> dataResourceIterator;

    public BulkDataLoader(Config config, Iterator<Tuple2<RocksDBRecord, String>> iterator, int partitionId) {
        this.partitionId = partitionId;
        this.dataResourceIterator = iterator;
        this.partitionCount = config.DBTablePartitionCount;

        this.bulkFilePrefix = config.destinationUrl + BULK_LOAD_DATA_ROOT + "/" + config.DBCluster + "/" + config.DBTableName + "/";
        this.partitionPath = bulkFilePrefix + "/" + partitionId + "/";
        this.bulkLoadInfoPath = bulkFilePrefix + "/" + BULK_LOAD_INFO;
        this.bulkLoadMetaDataPath = partitionPath + "/" + BULK_LOAD_METADATA;

        this.bulkLoadInfo = new BulkLoadInfo(config.DBCluster, config.DBTableName, config.DBTableId, config.DBTablePartitionCount);
        this.dataMetaInfo = new DataMetaInfo();

        this.fdsService = new FDSService();
        this.sstWriter = new SSTWriter(new RocksDBOptions(config), config.DBTablePartitionCount, partitionId);
    }


    void write()
            throws IOException, RocksDBException, URISyntaxException, FDSException {
        // TODO 在分布式环境/多线程环境中，createBulkLoadInfoFile会导致“bulkLoadInfo”文件被创建和写入多次
        createBulkLoadInfoFile();
        createSSTFile(dataResourceIterator);
        createBulkLoadMetaDataFile();
    }

    private void createSSTFile(Iterator<Tuple2<RocksDBRecord, String>> iterator) throws RocksDBException {
        String curSSTFileName = curSSTFileIndex + SST_SUFFIX;
        sstWriter.open(partitionPath + curSSTFileName);
        while (iterator.hasNext()) {
            RocksDBRecord rocksDBRecord = iterator.next()._1;
            if (curSSTFileSize > 64 * 1024 * 1024) {
                sstFileList.put(curSSTFileIndex + SST_SUFFIX, curSSTFileSize);
                curSSTFileIndex++;
                curSSTFileSize = 0L;
                curSSTFileName = curSSTFileIndex + SST_SUFFIX;
                sstWriter.close();
                System.out.println("pid=" + partitionId + ":file=" + curSSTFileName);
                sstWriter.open(partitionPath + curSSTFileName);
                System.out.println("pid=" + partitionId + ":open " + curSSTFileName);
            }
            curSSTFileSize += sstWriter.write(rocksDBRecord.key(), rocksDBRecord.value());
        }
        if (curSSTFileSize != 0) {
            sstWriter.close();
        } else {
            sstWriter.writeNoHashCheck("null", "null");
            sstWriter.close();
        }
        System.out.println("pid=" + partitionId + ":file=" + curSSTFileName);
    }

    private void createBulkLoadInfoFile() throws IOException, URISyntaxException, FDSException {
        BufferedWriter bulkLoadInfoWriter = fdsService.getWriter(bulkLoadInfoPath);
        bulkLoadInfoWriter.write(JSON.toJSONString(bulkLoadInfo));
        bulkLoadInfoWriter.close();
    }

    private void createBulkLoadMetaDataFile() throws IOException, URISyntaxException, FDSException {
        // todo
        int totalSize = 0;
        BufferedWriter bulkLoadMetaDataWriter = fdsService.getWriter(bulkLoadMetaDataPath);
        FileStatus[] fileStatuses = fdsService.getFileStatus(partitionPath);
        for (FileStatus fileStatus : fileStatuses) {
            String filePath = fileStatus.getPath().toString();

            String fileName = fileStatus.getPath().getName();
            long fileSize = fileStatus.getLen();
            String fileMD5 = fdsService.getMD5(filePath);

            FileInfo fileInfo = dataMetaInfo.new FileInfo(fileName, fileSize, fileMD5);
            dataMetaInfo.files.add(fileInfo);

            totalSize += fileSize;
        }
        dataMetaInfo.file_total_size = totalSize;
        bulkLoadMetaDataWriter.write(JSON.toJSONString(dataMetaInfo));
        bulkLoadMetaDataWriter.close();
    }


}
