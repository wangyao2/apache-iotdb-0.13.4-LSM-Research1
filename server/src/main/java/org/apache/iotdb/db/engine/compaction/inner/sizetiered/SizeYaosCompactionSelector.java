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

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.CompactionTaskManager;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * SizeTieredCompactionSelector selects files to be compacted based on the size of files. The
 * selector traverses the file list from old to new.
 * 旧版的方法，把文件列表里面的所有文件，按照从旧到新的顺序进行遍历
 * If the size of selected files or the number of
 * select files exceed given threshold, a compaction task will be submitted to task queue in
 * CompactionTaskManager.
 * 旧版的方法，挨个筛选，如果选择到的文件数量或者大小超过了某个阈值，那么就把这一批选中的文件提交到任务队列里面
 * In CompactionTaskManager, tasks are ordered by {@link
 * org.apache.iotdb.db.engine.compaction.CompactionTaskComparator}. To maximize compaction
 * efficiency, selector searches compaction task from 0 compaction files(that is, file that never
 * been compacted, named level 0 file) to higher level files.
 * 为了提高合并效率，文件选择器，从第0层开始选择文件，逐步向高层选取
 * If a compaction task is found in some
 * level, selector will not search higher level anymore. 如果当前层有文件被选取了，那么就不再去搜索跟高层了，而是为当前层生成一个合并任务
 */
public class SizeYaosCompactionSelector extends AbstractInnerSpaceCompactionSelector {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
    private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    public SizeYaosCompactionSelector(
            String logicalStorageGroupName,
            String virtualStorageGroupName,
            long timePartition,
            TsFileManager tsFileManager,
            boolean sequence,
            InnerSpaceCompactionTaskFactory taskFactory) {
        super(
                logicalStorageGroupName,
                virtualStorageGroupName,
                timePartition,
                tsFileManager,
                sequence,
                taskFactory);
    }

    /**
     * This method searches for a batch of files to be compacted from layer 0 to the highest layer. If
     * there are more than a batch of files to be merged on a certain layer, it does not search to
     * higher layers. It creates a compaction thread for each batch of files and put it into the
     * candidateCompactionTaskQueue of the {@link CompactionTaskManager}.
     *
     * @return Returns whether the file was found and submits the merge task
     */
    @Override
    public void selectAndSubmit() {
        PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue = //被选中的文件都存在taskPriorityQueue队列里面了
                new PriorityQueue<>(new SizeTieredCompactionTaskComparator());
        try {
            int maxLevel = searchMaxFileLevel();
            for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
                if (!selectLevelTask(currentLevel, taskPriorityQueue)) { //如果在一层中找到了一批可以合并的文件，那么就终止，不再判断其他层级了
                    break;//这里面包含了核心的执行选择合并任务的逻辑,直到当前层里面有就不去遍历下一层了，
                }
            }
            while (taskPriorityQueue.size() > 0) { //前面可能遍历得到了好几批，候选文件的集和，这里分别把他们提交成任务
                createAndSubmitTask(taskPriorityQueue.poll().left); //如果有待合并的文件，那么就就提交这个任务
            }
        } catch (Exception e) {
            LOGGER.error("Exception occurs while selecting files", e);
        }
    }

    /**
     * This method searches for all files on the given level. If there are consecutive files on the
     * level that meet the system preset conditions (the number exceeds 10 or the total file size
     * exceeds 2G),
     * 遍历一层的文件，如果累积的文件数量满足了10份或者超过了大小，那么就创建合并任务
     * a compaction task is created for the batch of files and placed in the
     * taskPriorityQueue queue , and continue to search for the next batch.
     * 创建合并任务后，再进一步搜索下一批
     * If at least one batch of
     * files to be compacted is found on this layer, it will return false (indicating that it will no
     * longer search for higher layers), otherwise it will return true.
     * 如果这一层里面找到了至少一批，那么就终止下一层的搜索
     *
     * @param level             the level to be searched
     * @param taskPriorityQueue it stores the batches of files to be compacted and the total size of
     *                          each batch
     * @return return whether to continue the search to higher levels
     * @throws IOException
     */
    private boolean selectLevelTask(
            int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
            throws IOException {
        boolean shouldContinueToSearch = true;
        List<TsFileResource> selectedFileList = new ArrayList<>();
        long selectedFileSize = 0L;
        long targetCompactionFileSize = config.getTargetCompactionFileSize(); // 1GB的字节

        for (TsFileResource currentFile : tsFileResources) {
            TsFileNameGenerator.TsFileName currentName = //把文件名进行解析成时间戳-版本-合并次数-跨空间次数的格式
                    TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
            if (currentName.getInnerCompactionCnt() != level //如果遍历的时候，跟当前处理的层级不一致，那么就跳过这个文件
                    || currentFile.getStatus() != TsFileResourceStatus.CLOSED) {
                selectedFileList.clear();
                selectedFileSize = 0L;
                continue;
            }
            LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
            selectedFileList.add(currentFile); //把当前层级的文件持续的添加到临时队列selectedFileList当中，只要没满足142行的条件，就一直添加新的进来
            selectedFileSize += currentFile.getTsFileSize();
            LOGGER.debug(
                    "Add tsfile {}, current select file num is {}, size is {}",
                    currentFile,
                    selectedFileList.size(),
                    selectedFileSize);
            // if the file size or file num reach threshold，判断临时队列的数量或者存储空间的大小
            if (selectedFileSize >= targetCompactionFileSize
                    || selectedFileList.size() >= config.getMaxInnerCompactionCandidateFileNum()) { //合并时候候选文件的数量如果为3，那么就合并，原本是30个合并
                // submit the task
                if (selectedFileList.size() > 1) {//满足刷写条件之后，就封装这一批文件到任务队列中
                    taskPriorityQueue.add(new Pair<>(new ArrayList<>(selectedFileList), selectedFileSize));
                }
                selectedFileList = new ArrayList<>();//然后清空临时队列，继续检测当前层级的其他文件是否仍然满足条件，直到遍历完所有的文件一遍
                selectedFileSize = 0L;
                shouldContinueToSearch = false;
            }
        }
        return shouldContinueToSearch;
    }

    private int searchMaxFileLevel() throws IOException {
        int maxLevel = -1; //根据文件的名称去判断待合并文件所属的层级
        Iterator<TsFileResource> iterator = tsFileResources.iterator();
        while (iterator.hasNext()) {
            TsFileResource currentFile = iterator.next();
            TsFileNameGenerator.TsFileName currentName =
                    TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
            if (currentName.getInnerCompactionCnt() > maxLevel) {//以内部的合并次数作为合并层级的记录
                maxLevel = currentName.getInnerCompactionCnt();
            }
        }
        return maxLevel;
    }

    private boolean createAndSubmitTask(List<TsFileResource> selectedFileList)
            throws InterruptedException {
        AbstractCompactionTask compactionTask =
                taskFactory.createTask(
                        logicalStorageGroupName,
                        virtualStorageGroupName,
                        timePartition,
                        tsFileManager,
                        selectedFileList,
                        sequence);
        return CompactionTaskManager.getInstance().addTaskToWaitingQueue(compactionTask);
    }

    private class SizeTieredCompactionTaskComparator
            implements Comparator<Pair<List<TsFileResource>, Long>> {

        @Override
        public int compare(Pair<List<TsFileResource>, Long> o1, Pair<List<TsFileResource>, Long> o2) {
            TsFileResource resourceOfO1 = o1.left.get(0);
            TsFileResource resourceOfO2 = o2.left.get(0);
            try {
                TsFileNameGenerator.TsFileName fileNameOfO1 =
                        TsFileNameGenerator.getTsFileName(resourceOfO1.getTsFile().getName());
                TsFileNameGenerator.TsFileName fileNameOfO2 =
                        TsFileNameGenerator.getTsFileName(resourceOfO2.getTsFile().getName());
                if (fileNameOfO1.getInnerCompactionCnt() != fileNameOfO2.getInnerCompactionCnt()) { //如果两个文件内部合并的次数不一样
                    return fileNameOfO2.getInnerCompactionCnt() - fileNameOfO1.getInnerCompactionCnt();//合并层级小的优先级高
                }
                return (int) (fileNameOfO2.getVersion() - fileNameOfO1.getVersion());//版本小的优先级高
            } catch (IOException e) {
                return 0;
            }
        }
    }
}
//  如果你返回一个负数(-ve)，表示前面的对象小于后面的对象。
//  如果你返回一个正数(+ve)，表示前面的对象大于后面的对象。