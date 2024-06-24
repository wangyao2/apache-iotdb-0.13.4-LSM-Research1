/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunk;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.VirtualStorageGroupProcessor;
import org.apache.iotdb.db.exception.WriteProcessException;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.idtable.entry.DeviceIDFactory;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.DeletePlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertTabletPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.writelog.io.ILogReader;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * LogReplayer finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the
 * WALs from the logNode and redoes them into a given MemTable and ModificationFile.
 */
public class LogReplayer {

  private Logger logger = LoggerFactory.getLogger(LogReplayer.class);
  private String logNodePrefix;
  private String insertFilePath;
  private ModificationFile modFile;
  private TsFileResource currentTsFileResource;
  private IMemTable recoverMemTable;

  // only unsequence file tolerates duplicated data
  private boolean sequence;

  public LogReplayer(
      String logNodePrefix,
      String insertFilePath,
      ModificationFile modFile,
      TsFileResource currentTsFileResource,
      IMemTable memTable,
      boolean sequence) {
    this.logNodePrefix = logNodePrefix;
    this.insertFilePath = insertFilePath;
    this.modFile = modFile;
    this.currentTsFileResource = currentTsFileResource;
    this.recoverMemTable = memTable;
    this.sequence = sequence;
  }

  /**
   * finds the logNode of the TsFile given by insertFilePath and logNodePrefix, reads the WALs from
   * the logNode and redoes them into a given MemTable and ModificationFile.
   */
  public void replayLogs(
      Supplier<ByteBuffer[]> supplier, VirtualStorageGroupProcessor virtualStorageGroupProcessor) {
    WriteLogNode logNode =
        MultiFileLogNodeManager.getInstance()
            .getNode(
                logNodePrefix + FSFactoryProducer.getFSFactory().getFile(insertFilePath).getName(),
                supplier);

    ILogReader logReader = logNode.getLogReader();
    try {
      while (logReader.hasNext()) {
        try {
          PhysicalPlan plan = logReader.next();
          if (plan instanceof InsertPlan) {
            replayInsert((InsertPlan) plan, virtualStorageGroupProcessor);
          } else if (plan instanceof DeletePlan) {
            replayDelete((DeletePlan) plan);
          }
        } catch (PathNotExistException ignored) {
          // can not get path because it is deleted
        } catch (Exception e) {
          logger.warn("recover wal of {} failed", insertFilePath, e);
        }
      }
    } catch (IOException e) {
      logger.warn("meet error when redo wal of {}", insertFilePath, e);
    } finally {
      logReader.close();
      try {
        modFile.close();
      } catch (IOException e) {
        logger.error("Cannot close the modifications file {}", modFile.getFilePath(), e);
      }
    }

    Map<IDeviceID, IWritableMemChunkGroup> memTableMap = recoverMemTable.getMemTableMap();
    for (Map.Entry<IDeviceID, IWritableMemChunkGroup> deviceEntry : memTableMap.entrySet()) {
      String deviceId = deviceEntry.getKey().toStringID();
      for (Map.Entry<String, IWritableMemChunk> measurementEntry :
          deviceEntry.getValue().getMemChunkMap().entrySet()) {
        IWritableMemChunk memChunk = measurementEntry.getValue();
        currentTsFileResource.updateStartTime(deviceId, memChunk.getFirstPoint());
        currentTsFileResource.updateEndTime(deviceId, memChunk.getLastPoint());
      }
    }
  }

  private void replayDelete(DeletePlan deletePlan) throws IOException, MetadataException {
    List<PartialPath> paths = deletePlan.getPaths();
    for (PartialPath path : paths) {
      for (PartialPath device : IoTDB.metaManager.getBelongedDevices(path)) {
        recoverMemTable.delete(
            path, device, deletePlan.getDeleteStartTime(), deletePlan.getDeleteEndTime());
      }
      modFile.write(
          new Deletion(
              path,
              currentTsFileResource.getTsFileSize(),
              deletePlan.getDeleteStartTime(),
              deletePlan.getDeleteEndTime()));
    }
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  private void replayInsert(
      InsertPlan plan, VirtualStorageGroupProcessor virtualStorageGroupProcessor)
      throws WriteProcessException, QueryProcessException {
    if (!plan.hasValidMeasurements()) {
      return;
    }
    if (currentTsFileResource != null) {
      long minTime;
      if (plan instanceof InsertRowPlan) {
        minTime = ((InsertRowPlan) plan).getTime();
      } else {
        minTime = plan.getMinTime();
      }
      String deviceId =
          plan.isAligned()
              ? plan.getDevicePath().getDevicePath().getFullPath()
              : plan.getDevicePath().getFullPath();
      // the last chunk group may contain the same data with the logs, ignore such logs in seq file
      long lastEndTime = currentTsFileResource.getEndTime(deviceId);
      if (lastEndTime != Long.MIN_VALUE && lastEndTime >= minTime && sequence) {
        return;
      }
    }

    plan.setMeasurementMNodes(new IMeasurementMNode[plan.getMeasurements().length]);
    try {
      if (IoTDBDescriptor.getInstance().getConfig().isEnableIDTable()) {
        virtualStorageGroupProcessor.getIdTable().getSeriesSchemas(plan);
      } else {
        IoTDB.metaManager.getSeriesSchemasAndReadLockDevice(plan);
        plan.setDeviceID(DeviceIDFactory.getInstance().getDeviceID(plan.getDevicePath()));
      }
    } catch (IOException | MetadataException e) {
      throw new QueryProcessException("can't replay insert logs, ", e);
    }

    // mark failed plan manually
    checkDataTypeAndMarkFailed(plan.getMeasurementMNodes(), plan);
    if (plan instanceof InsertRowPlan) {
      if (plan.isAligned()) {
        recoverMemTable.insertAlignedRow((InsertRowPlan) plan);
      } else {
        recoverMemTable.insert((InsertRowPlan) plan);
      }
    } else {
      if (plan.isAligned()) {
        recoverMemTable.insertAlignedTablet(
            (InsertTabletPlan) plan, 0, ((InsertTabletPlan) plan).getRowCount());
      } else {
        recoverMemTable.insertTablet(
            (InsertTabletPlan) plan, 0, ((InsertTabletPlan) plan).getRowCount());
      }
    }
  }

  private void checkDataTypeAndMarkFailed(final IMeasurementMNode[] mNodes, InsertPlan tPlan) {
    for (int i = 0; i < mNodes.length; i++) {
      if (mNodes[i] == null) {
        tPlan.markFailedMeasurementInsertion(
            i,
            new PathNotExistException(
                tPlan.getDevicePath().getFullPath()
                    + IoTDBConstant.PATH_SEPARATOR
                    + tPlan.getMeasurements()[i]));
      } else if (mNodes[i].getSchema().getType() != tPlan.getDataTypes()[i]) {
        tPlan.markFailedMeasurementInsertion(
            i,
            new DataTypeMismatchException(
                mNodes[i].getName(), tPlan.getDataTypes()[i], mNodes[i].getSchema().getType()));
      }
    }
  }
}
