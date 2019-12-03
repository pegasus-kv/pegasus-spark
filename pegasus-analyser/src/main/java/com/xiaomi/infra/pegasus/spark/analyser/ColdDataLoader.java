package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.core.Config;
import com.xiaomi.infra.pegasus.spark.core.FDSException;
import com.xiaomi.infra.pegasus.spark.core.FDSService;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONException;
import org.json.JSONObject;

public class ColdDataLoader implements Serializable {
  private static final Log LOG = LogFactory.getLog(ColdDataLoader.class);

  private FDSService fdsService = new FDSService();
  private Map<Integer, String> checkpointUrls = new HashMap<>();
  private int partitionCount;
  private Config globalConfig;
  private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

  public ColdDataLoader(Config config) throws FDSException {
    globalConfig = config;

    String idPrefix =
        globalConfig.destinationUrl
            + "/"
            + globalConfig.DBCluster
            + "/"
            + globalConfig.DBColdBackUpPolicy
            + "/";
    String latestIdPath = getLatestPolicyId(idPrefix);
    String tableNameAndId = getTableNameAndId(latestIdPath, globalConfig.DBTableName);
    String metaPrefix = latestIdPath + "/" + tableNameAndId;

    partitionCount = getCount(metaPrefix);
    initCheckpointUrls(metaPrefix, partitionCount);

    LOG.info("init fds default config and get the latest data urls");
  }

  public ColdDataLoader(Config config, String dataTime) throws FDSException {
    globalConfig = config;
    String idPrefix =
        globalConfig.destinationUrl
            + "/"
            + globalConfig.DBCluster
            + "/"
            + globalConfig.DBColdBackUpPolicy
            + "/";
    String idPath = getPolicyId(idPrefix, dataTime);
    String tableNameAndId = getTableNameAndId(idPath, globalConfig.DBTableName);
    String metaPrefix = idPath + "/" + tableNameAndId;

    partitionCount = getCount(metaPrefix);
    initCheckpointUrls(metaPrefix, partitionCount);

    LOG.info("init fds default config and get the " + dataTime + " data urls");
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public Map<Integer, String> getCheckpointUrls() {
    return checkpointUrls;
  }

  private void initCheckpointUrls(String prefix, int counter) throws FDSException {
    String chkpt;
    counter--;
    while (counter >= 0) {
      String currentCheckpointUrl = prefix + "/" + counter + "/" + "current_checkpoint";
      try (InputStream inputStream =
              FileSystem.get(new URI(currentCheckpointUrl), new Configuration())
                  .open(new Path(currentCheckpointUrl));
          BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
        while ((chkpt = bufferedReader.readLine()) != null) {
          String url = prefix.split(globalConfig.destinationUrl)[1] + "/" + counter + "/" + chkpt;
          checkpointUrls.put(counter, url);
        }
        counter--;
      } catch (IOException | URISyntaxException e) {
        LOG.error("init checkPoint urls failed from " + currentCheckpointUrl);
        throw new FDSException(
            "init checkPoint urls failed, [checkpointUrl:" + currentCheckpointUrl + "]" + e);
      }
    }
  }

  private String getLatestPolicyId(String prefix) throws FDSException {
    try {
      LOG.info("get the " + prefix + " latest id");
      ArrayList<String> idList = getPolicyIdList(fdsService.getFileStatus(prefix));
      LOG.info("the policy list:" + idList);
      if (idList.size() != 0) {
        return idList.get(idList.size() - 1);
      }
    } catch (IOException e) {
      LOG.error("get latest policy id from " + prefix + "failed!");
      throw new FDSException("get latest policy id failed, [url:" + prefix + "]", e);
    }
    LOG.error("get latest policy id from " + prefix + " failed, no policy id existed!");
    throw new FDSException(
        "get latest policy id from " + prefix + " failed, no policy id existed!");
  }

  private ArrayList<String> getPolicyIdList(FileStatus[] status) {
    ArrayList<String> idList = new ArrayList<>();
    for (FileStatus fileStatus : status) {
      idList.add(fileStatus.getPath().toString());
    }
    return idList;
  }

  private String getPolicyId(String prefix, String dateTime) throws FDSException {
    try {
      FileStatus[] fileStatuses = fdsService.getFileStatus(prefix);
      for (FileStatus s : fileStatuses) {
        String idPath = s.getPath().toString();
        long timestamp = Long.parseLong(idPath.substring(idPath.length() - 13));
        String date = simpleDateFormat.format(new Date(timestamp));
        if (date.equals(dateTime)) {
          return idPath;
        }
      }
    } catch (IOException e) {
      LOG.error("get latest policy id from " + prefix + "failed!");
      throw new FDSException("get latest policy id failed, [url:" + prefix + "]", e);
    }
    throw new FDSException("can't match the date time:+" + dateTime);
  }

  private String getTableNameAndId(String prefix, String tableName) throws FDSException {
    String backupInfo;
    String backupInfoUrl = prefix + "/" + "backup_info";
    try (BufferedReader bufferedReader = fdsService.getReader(backupInfoUrl)) {
      while ((backupInfo = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(backupInfo);
        JSONObject tables = jsonObject.getJSONObject("app_names");
        Iterator<String> iterator = tables.keys();
        while (iterator.hasNext()) {
          String tableId = iterator.next();
          if (tables.get(tableId).equals(tableName)) {
            return tableName + "_" + tableId;
          }
        }
      }
    } catch (IOException | JSONException e) {
      LOG.error("get name and id from " + prefix + " failed!");
      throw new FDSException("get name and id failed, [url:" + prefix + "]", e);
    }
    throw new FDSException("can't get the table name and id");
  }

  private int getCount(String prefix) throws FDSException {
    String appMetaData;
    String appMetaDataUrl = prefix + "/" + "meta" + "/" + "app_metadata";
    try {
      BufferedReader bufferedReader = fdsService.getReader(appMetaDataUrl);
      while ((appMetaData = bufferedReader.readLine()) != null) {
        JSONObject jsonObject = new JSONObject(appMetaData);
        String partitionCount = jsonObject.getString("partition_count");
        return Integer.parseInt(partitionCount);
      }
    } catch (IOException | JSONException e) {
      LOG.error("get the partition count failed from " + appMetaDataUrl, e);
      throw new FDSException("get the partition count failed, [url: " + appMetaDataUrl + "]" + e);
    }
    throw new FDSException("get the partition count failed, [url: " + appMetaDataUrl + "]");
  }
}
