/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.compaction.inner.sizetiered;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.engine.compaction.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.InnerSpaceCompactionUtils;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.compaction.task.CompactionExceptionHandler;
import org.apache.iotdb.db.engine.compaction.utils.log.CompactionLogger;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.TsFileNotCompleteException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SizeTiredCompactionTask compact several inner space files selected by {@link
 * SizeTieredCompactionSelector} into one file.
 */
public class SizeTieredCompactionTask extends AbstractInnerSpaceCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
  protected TsFileResourceList tsFileResourceList;
  protected boolean[] isHoldingReadLock;
  protected boolean[] isHoldingWriteLock;

  public SizeTieredCompactionTask(
      String logicalStorageGroupName,
      String virtualStorageGroupName,
      long timePartition,
      TsFileManager tsFileManager,
      List<TsFileResource> selectedTsFileResourceList,
      boolean sequence,
      AtomicInteger currentTaskNum) {
    super(
        logicalStorageGroupName + "-" + virtualStorageGroupName,
        timePartition,
        currentTaskNum,
        sequence,
        selectedTsFileResourceList,
        tsFileManager);
    isHoldingReadLock = new boolean[selectedTsFileResourceList.size()];
    isHoldingWriteLock = new boolean[selectedTsFileResourceList.size()];
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      isHoldingWriteLock[i] = false;
      isHoldingReadLock[i] = false;
    }
    if (sequence) {
      tsFileResourceList = tsFileManager.getSequenceListByTimePartition(timePartition);
    } else {
      tsFileResourceList = tsFileManager.getUnsequenceListByTimePartition(timePartition);
    }
  }

  @Override
  protected void doCompaction() throws Exception {
    if (!tsFileManager.isAllowCompaction()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    // get resource of target file
    String dataDirectory = selectedTsFileResourceList.get(0).getTsFile().getParent();
    // Here is tmpTargetFile, which is xxx.target
    TsFileResource targetTsFileResource =
        TsFileNameGenerator.getInnerCompactionTargetFileResource( //创建合并后的临时目标文件，这方法里面主要是分析所有文件的起始时间和名字，然后给新的目标文件命名
            selectedTsFileResourceList, sequence);
    List<TsFileResource> targetTsFileList =
        new ArrayList<>(Collections.singletonList(targetTsFileResource)); //只有一个元素的列表，意义不明，可能是为了适配某些方法的形参
    LOGGER.info(
        "{} [Compaction] starting compaction task with {} files",
        fullStorageGroupName,
        selectedTsFileResourceList.size());
    File logFile =
        new File(
            dataDirectory
                + File.separator
                + targetTsFileResource.getTsFile().getName()
                + CompactionLogger.INNER_COMPACTION_LOG_NAME_SUFFIX);
    CompactionLogger sizeTieredCompactionLogger = null;
    try {
      sizeTieredCompactionLogger = new CompactionLogger(logFile);
      sizeTieredCompactionLogger.logFiles( //向日志文件中写入数据
          selectedTsFileResourceList, CompactionLogger.STR_SOURCE_FILES);
      sizeTieredCompactionLogger.logFiles(targetTsFileList, CompactionLogger.STR_TARGET_FILES);
      LOGGER.info("{} [SizeTiredCompactionTask] Close the logger", fullStorageGroupName);
      sizeTieredCompactionLogger.close();
      LOGGER.info(
          "{} [Compaction] compaction with {}", fullStorageGroupName, selectedTsFileResourceList);

      // carry out the compaction
      if (sequence) {
        InnerSpaceCompactionUtils.compact(targetTsFileResource, selectedTsFileResourceList);
      } else {
        CompactionUtils.compact(
            Collections.emptyList(), selectedTsFileResourceList, targetTsFileList);
      }

      InnerSpaceCompactionUtils.moveTargetFile(targetTsFileResource, fullStorageGroupName);

      LOGGER.info("{} [SizeTiredCompactionTask] start to rename mods file", fullStorageGroupName);
      InnerSpaceCompactionUtils.combineModsInCompaction(
          selectedTsFileResourceList, targetTsFileResource);

      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException(
            String.format("%s [Compaction] abort", fullStorageGroupName));
      }

      // replace the old files with new file, the new is in same position as the old
      if (sequence) {
        tsFileManager.replace(
            selectedTsFileResourceList,
            Collections.emptyList(),
            targetTsFileList,
            timePartition,
            true);
      } else {
        tsFileManager.replace(
            Collections.emptyList(),
            selectedTsFileResourceList,
            targetTsFileList,
            timePartition,
            false);
      }

      LOGGER.info(
          "{} [Compaction] Compacted target files, try to get the write lock of source files",
          fullStorageGroupName);

      // release the read lock of all source files, and get the write lock of them to delete them
      for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
        selectedTsFileResourceList.get(i).readUnlock();
        isHoldingReadLock[i] = false;
        selectedTsFileResourceList.get(i).writeLock();
        isHoldingWriteLock[i] = true;
      }

      if (targetTsFileResource.getTsFile().exists()
          && targetTsFileResource.getTsFile().length()
              < TSFileConfig.MAGIC_STRING.getBytes().length * 2L + Byte.BYTES) {
        // the file size is smaller than magic string and version number
        throw new TsFileNotCompleteException(
            String.format(
                "target file %s is smaller than magic string and version number size",
                targetTsFileResource));
      }

      LOGGER.info(
          "{} [Compaction] compaction finish, start to delete old files", fullStorageGroupName);
      // delete the old files
      InnerSpaceCompactionUtils.deleteTsFilesInDisk(
          selectedTsFileResourceList, fullStorageGroupName);
      InnerSpaceCompactionUtils.deleteModificationForSourceFile(
          selectedTsFileResourceList, fullStorageGroupName);

      double costTime = (System.currentTimeMillis() - startTime) / 1000.0d;
      LOGGER.info(
          "{} [SizeTiredCompactionTask] all compaction task finish, target file is {},"
              + "time cost is {} s, compaction speed is {} MB/s",
          fullStorageGroupName,
          targetTsFileResource.getTsFile().getName(),
          costTime,
          ((double) selectedFileSize) / 1024.0d / 1024.0d / costTime);

      if (logFile.exists()) {
        FileUtils.delete(logFile);
      }

      if (targetTsFileResource.isDeleted()) {
        // target resource is empty after compaction, then delete it
        targetTsFileResource.remove();
      } else {
        // set target resource to CLOSED, so that it can be selected to compact
        targetTsFileResource.setStatus(TsFileResourceStatus.CLOSED);
      }
    } catch (Throwable throwable) {
      LOGGER.warn("{} [Compaction] Start to handle exception", fullStorageGroupName);
      if (sizeTieredCompactionLogger != null) {
        sizeTieredCompactionLogger.close();
      }
      if (isSequence()) {
        CompactionExceptionHandler.handleException(
            fullStorageGroupName,
            logFile,
            targetTsFileList,
            selectedTsFileResourceList,
            Collections.emptyList(),
            tsFileManager,
            timePartition,
            true,
            isSequence());
      } else {
        CompactionExceptionHandler.handleException(
            fullStorageGroupName,
            logFile,
            targetTsFileList,
            Collections.emptyList(),
            selectedTsFileResourceList,
            tsFileManager,
            timePartition,
            true,
            isSequence());
      }
      throw throwable;
    } finally {
      releaseFileLocksAndResetMergingStatus();
    }
  }

  @Override
  public boolean equalsOtherTask(AbstractCompactionTask other) {
    if (other instanceof SizeTieredCompactionTask) {
      SizeTieredCompactionTask otherSizeTieredTask = (SizeTieredCompactionTask) other;
      if (!selectedTsFileResourceList.equals(otherSizeTieredTask.selectedTsFileResourceList)) {
        return false;
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean checkValidAndSetMerging() {
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      TsFileResource resource = selectedTsFileResourceList.get(i);
      resource.readLock();
      isHoldingReadLock[i] = true;
      if (resource.isCompacting()
          || !resource.isClosed()
          || !resource.getTsFile().exists()
          || resource.isDeleted()) {
        // this source file cannot be compacted
        // release the lock of locked files, and return
        releaseFileLocksAndResetMergingStatus();
        return false;
      }
    }

    for (TsFileResource resource : selectedTsFileResourceList) {
      resource.setStatus(TsFileResourceStatus.COMPACTING);
    }
    return true;
  }

  /**
   * release the read lock and write lock of files if it is held, and set the merging status of
   * selected files to false
   */
  private void releaseFileLocksAndResetMergingStatus() {
    for (int i = 0; i < selectedTsFileResourceList.size(); ++i) {
      if (isHoldingReadLock[i]) {
        selectedTsFileResourceList.get(i).readUnlock();
      }
      if (isHoldingWriteLock[i]) {
        selectedTsFileResourceList.get(i).writeUnlock();
      }
      selectedTsFileResourceList.get(i).setStatus(TsFileResourceStatus.CLOSED);
    }
  }
}