/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.compaction.inner.utils;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.CrossCompactionStrategy;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.manage.CrossSpaceCompactionResource;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.ICrossSpaceMergeFileSelector;
import org.apache.iotdb.db.engine.compaction.cross.rewrite.selector.RewriteCompactionFileSelector;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.rescon.SystemInfo;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class InnerSpaceCompactionUtils {

  private static final Logger logger =
      LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);

  private InnerSpaceCompactionUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void compact(TsFileResource targetResource, List<TsFileResource> tsFileResources)
      throws IOException, MetadataException, InterruptedException {

    // size for file writer is 5% of per compaction task memory budget
    long sizeForFileWriter = //原来这个是最大元数据的大小，从writer的入参推断出来
        (long)
            (((double) SystemInfo.getInstance().getMemorySizeForCompaction()
                    / (double)
                        IoTDBDescriptor.getInstance().getConfig().getConcurrentCompactionThread())
                * IoTDBDescriptor.getInstance().getConfig().getChunkMetadataMemorySizeProportion()); //deviceIterator 在创建对象的时候还封装了每一个文件对应的设备信息
    try (MultiTsFileDeviceIterator deviceIterator = new MultiTsFileDeviceIterator(tsFileResources); //对象里面封装了所有待合并的文件资源，还有每个文件对应了一个reader的fileChannel
        TsFileIOWriter writer = //为待写入的文件设置一个 writer准备向里面写入内容，创建对象的时候会在里面自动写入文件头，magic和版本信息
            new TsFileIOWriter(targetResource.getTsFile(), true, sizeForFileWriter)) {
      while (deviceIterator.hasNextDevice()) { //处理文件资源中出现的每一个设备
        Pair<String, Boolean> deviceInfo = deviceIterator.nextDevice();
        String device = deviceInfo.left;
        boolean aligned = deviceInfo.right;

        if (aligned) {
          compactAlignedSeries(device, targetResource, writer, deviceIterator);
        } else {
          writer.startChunkGroup(device); //一个chunkgroup对应一个设备，一个设备写一组，每写入一个设备就新开一个chunkgroup
          compactNotAlignedSeries(device, targetResource, writer, deviceIterator);
          writer.endChunkGroup(); //写完设备之后就关闭这个chunkGroup
        }
      }

      for (TsFileResource tsFileResource : tsFileResources) {
        targetResource.updatePlanIndexes(tsFileResource);
      }
      writer.endFile(); //关闭文件的时候会构建索引树
    }
  }

  private static void checkThreadInterrupted(TsFileResource tsFileResource)
      throws InterruptedException {
    if (Thread.interrupted() || !IoTDB.activated) {
      throw new InterruptedException(
          String.format(
              "[Compaction] compaction for target file %s abort", tsFileResource.toString()));
    }
  }

  private static void compactNotAlignedSeries( //2024年6月14日》7月13日又尝试继续继续研究合并一个设备数据的流程和方法
      String device, //待处理的当前设备，对应一个chunkgroup
      TsFileResource targetResource, //被写入的目标文件,也就是合并的结果文件
      TsFileIOWriter writer,//目标文件的writer 合并的结果文件对应的IOWriter
      MultiTsFileDeviceIterator deviceIterator) //所有候选文件里面的设备迭代器
      throws IOException, MetadataException, InterruptedException {
    MultiTsFileDeviceIterator.MeasurementIterator seriesIterator = //获得当前设备的所有序列迭代器
        deviceIterator.iterateNotAlignedSeries(device, true);
    while (seriesIterator.hasNextSeries()) { //遍历每一条传感器序列,针对一个具体的设备，再进一步找里面的序列和chunk元数据
      checkThreadInterrupted(targetResource);
      // TODO: we can provide a configuration item to enable concurrent between each series
      PartialPath p = new PartialPath(device, seriesIterator.nextSeries());//把完整的root.cpt.g0.g0.s0切成一个一个的路径节点，同时在nextSeries方法内会替换到下一个序列s0
      IMeasurementSchema measurementSchema;
      // TODO: seriesIterator needs to be refactor.
      // This statement must be called before next hasNextSeries() called, or it may be trapped in a
      // dead-loop.
      LinkedList<Pair<TsFileSequenceReader, List<ChunkMetadata>>> readerAndChunkMetadataList = //再记录了一个设备比如d0,里面的某一个序列比如s0。所有文件的里面的，对应的所有的chunk的偏移量，记录到chunkmetada内
          seriesIterator.getMetadataListForCurrentSeries();
      SingleSeriesCompactionExecutor compactionExecutorOfCurrentTimeSeries = //SingleSeriesCompactionExecutor这个类用于合并inner空间内的序列
          new SingleSeriesCompactionExecutor(p, readerAndChunkMetadataList, writer, targetResource);
      compactionExecutorOfCurrentTimeSeries.execute(); //单一条序列的合并逻辑子这里面，对应了文档里面的核心逻辑内容
    }
    writer.checkMetadataSizeAndMayFlush();
  }

  private static void compactAlignedSeries(
      String device,
      TsFileResource targetResource,
      TsFileIOWriter writer,
      MultiTsFileDeviceIterator deviceIterator)
      throws IOException, InterruptedException {
    checkThreadInterrupted(targetResource);
    LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>> readerAndChunkMetadataList =
        deviceIterator.getReaderAndChunkMetadataForCurrentAlignedSeries();
    if (!checkAlignedSeriesValid(readerAndChunkMetadataList)) {
      return;
    }
    AlignedSeriesCompactionExecutor compactionExecutor =
        new AlignedSeriesCompactionExecutor(
            device, targetResource, readerAndChunkMetadataList, writer);
    compactionExecutor.execute();
  }

  /** Ensure that there is at least one chunk that is not empty. */
  private static boolean checkAlignedSeriesValid(
      LinkedList<Pair<TsFileSequenceReader, List<AlignedChunkMetadata>>>
          readerAndChunkMetadataList) {
    for (Pair<TsFileSequenceReader, List<AlignedChunkMetadata>> readerMetadataPair :
        readerAndChunkMetadataList) {
      if (!readerMetadataPair.right.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  public static boolean deleteTsFilesInDisk(
      Collection<TsFileResource> mergeTsFiles, String storageGroupName) {
    logger.info("{} [Compaction] Compaction starts to delete real file ", storageGroupName);
    boolean result = true;
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      if (!deleteTsFile(mergeTsFile)) {
        result = false;
      }
      logger.info(
          "{} [Compaction] delete TsFile {}", storageGroupName, mergeTsFile.getTsFilePath());
    }
    return result;
  }

  /** Delete all modification files for source files */
  public static void deleteModificationForSourceFile(
      Collection<TsFileResource> sourceFiles, String storageGroupName) throws IOException {
    logger.info("{} [Compaction] Start to delete modifications of source files", storageGroupName);
    for (TsFileResource tsFileResource : sourceFiles) {
      ModificationFile compactionModificationFile =
          ModificationFile.getCompactionMods(tsFileResource);
      if (compactionModificationFile.exists()) {
        compactionModificationFile.remove();
      }

      ModificationFile normalModification = ModificationFile.getNormalMods(tsFileResource);
      if (normalModification.exists()) {
        normalModification.remove();
      }
    }
  }

  /**
   * Collect all the compaction modification files of source files, and combines them as the
   * modification file of target file.
   */
  public static void combineModsInCompaction(
      Collection<TsFileResource> mergeTsFiles, TsFileResource targetTsFile) throws IOException {
    List<Modification> modifications = new ArrayList<>();
    for (TsFileResource mergeTsFile : mergeTsFiles) {
      try (ModificationFile sourceCompactionModificationFile =
          ModificationFile.getCompactionMods(mergeTsFile)) {
        modifications.addAll(sourceCompactionModificationFile.getModifications());
      }
    }
    if (!modifications.isEmpty()) {
      try (ModificationFile modificationFile = ModificationFile.getNormalMods(targetTsFile)) {
        for (Modification modification : modifications) {
          // we have to set modification offset to MAX_VALUE, as the offset of source chunk may
          // change after compaction
          modification.setFileOffset(Long.MAX_VALUE);
          modificationFile.write(modification);
        }
      }
    }
  }

  public static boolean deleteTsFile(TsFileResource seqFile) {
    try {
      FileReaderManager.getInstance().closeFileAndRemoveReader(seqFile.getTsFilePath());
      seqFile.setStatus(TsFileResourceStatus.DELETED);
      seqFile.delete();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      return false;
    }
    return true;
  }

  public static ICrossSpaceMergeFileSelector getCrossSpaceFileSelector(
      long budget, CrossSpaceCompactionResource resource) {
    CrossCompactionStrategy strategy =
        IoTDBDescriptor.getInstance().getConfig().getCrossCompactionStrategy();
    switch (strategy) {
      case REWRITE_COMPACTION:
        return new RewriteCompactionFileSelector(resource, budget);
      default:
        throw new UnsupportedOperationException("Unknown CrossSpaceFileStrategy " + strategy);
    }
  }

  /**
   * Update the targetResource. Move xxx.target to xxx.tsfile and serialize xxx.tsfile.resource .
   *
   * @param targetResource the old tsfile to be moved, which is xxx.target
   */
  public static void moveTargetFile(TsFileResource targetResource, String fullStorageGroupName)
      throws IOException {
    if (!targetResource.getTsFile().exists()) {
      logger.info(
          "{} [Compaction] Tmp target tsfile {} may be deleted after compaction.",
          fullStorageGroupName,
          targetResource.getTsFilePath());
      return;
    }
    if (!targetResource.getTsFilePath().endsWith(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX)) {
      logger.warn(
          "{} [Compaction] Tmp target tsfile {} should be end with {}",
          fullStorageGroupName,
          targetResource.getTsFilePath(),
          IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX);
      return;
    }
    File oldFile = targetResource.getTsFile();

    // move TsFile and delete old tsfile
    String newFilePath =
        targetResource
            .getTsFilePath()
            .replace(IoTDBConstant.INNER_COMPACTION_TMP_FILE_SUFFIX, TsFileConstant.TSFILE_SUFFIX);
    File newFile = new File(newFilePath);
    FSFactoryProducer.getFSFactory().moveFile(oldFile, newFile);

    // serialize xxx.tsfile.resource
    targetResource.setFile(newFile);
    targetResource.serialize();
    targetResource.closeWithoutSettingStatus();
  }
}
