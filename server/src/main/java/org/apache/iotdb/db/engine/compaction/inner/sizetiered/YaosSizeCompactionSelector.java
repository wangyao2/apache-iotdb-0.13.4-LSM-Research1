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
import org.apache.iotdb.db.engine.storagegroup.timeindex.ITimeIndex;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

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
public class YaosSizeCompactionSelector extends AbstractInnerSpaceCompactionSelector {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
    private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

    private final long queryTimeInterval = 100; //这个参数在zhanglingzhe的代码中单独作为一个静态参数，据说是根据python分析的结果反写回来的

    public YaosSizeCompactionSelector(
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
                if (!selectLevelTask_byYaos_V1(currentLevel, taskPriorityQueue)) { //如果在一层中找到了一批可以合并的文件，那么就终止，不再判断其他层级了
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

    /*
    编写论文算法设计的，文件选择合并策略
     */
    private boolean selectLevelTask_byYaos_V1(
            int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue)
            throws IOException {
        boolean shouldContinueToSearch = true;
        List<TsFileResource> selectedFileList = new ArrayList<>();
        long selectedFileSize = 0L;
        long targetCompactionFileSize = config.getTargetCompactionFileSize(); // 1GB的字节

        for (TsFileResource currentFile : tsFileResources) { //获取顺序空间内的所有文件，或者乱序空间内的所有文件其一
            TsFileNameGenerator.TsFileName currentName = //把文件名进行解析成时间戳-版本-合并次数-跨空间次数的格式，判断是否处于当前层级
                    TsFileNameGenerator.getTsFileName(currentFile.getTsFile().getName());
            if (currentName.getInnerCompactionCnt() != level //如果遍历的时候，跟当前处理的层级不一致，那么就跳过这个文件
                    || currentFile.getStatus() != TsFileResourceStatus.CLOSED) { //或者文件不是关闭状态，就跳过
                selectedFileList.clear();
                selectedFileSize = 0L;
                continue;
            }
            LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());

            ////==========核心逻辑的编码位置=================
            // 现在的文件添加和选择方法是按照顺序选择，我们在这个等号的范围内编写自己的文件选择策略
            double mergeSpeed = 1;//这两个写入 合并 速率参数，暂时还不能确定具体数据
            double writeSpeed = 0;//原版本里也没有这个参数
            List<long[]> candidateList = new ArrayList<>();//仅仅记录了下标和位置
            List<TsFileResource> overlappedList = calculateOverlappedList(tsFileResources);
            long offsetTime = 0;
            for (int i = 0; i < overlappedList.size(); i++) { //遍历每一个有交叉的文件
                TsFileResource tsFileResource = overlappedList.get(i);
                long mergedTimeInterval = tsFileResource.getTimeIndex().getMaxEndTime() - tsFileResource.getTimeIndex().getMinStartTime();//这里原本是根据tsfileresource去计算起止时间
                long mergeTimeCost = (long) (tsFileResource.getTsFileSize() / mergeSpeed * writeSpeed);
                if (queryTimeInterval < (mergedTimeInterval + mergeTimeCost + offsetTime)) {
                    continue;
                }
                offsetTime += mergedTimeInterval;
                for (int j = i + 1; j < overlappedList.size(); j++) { //似乎是遍历i后面的每一个文件
                    TsFileResource endTsFileResource = overlappedList.get(j);
                    mergeTimeCost += endTsFileResource.getTsFileSize() / mergeSpeed * writeSpeed;
                    long allReward = 0L;
                    int maxReward = j - i;
                    long fullRewardTime = queryTimeInterval - offsetTime - mergedTimeInterval - mergeTimeCost;
                    allReward += maxReward * fullRewardTime;
                    if (allReward > 0) {
                        // calculate not full reward time, from 1 to max_reward, which is active as long as the interval of every file
                        for (int k = 0; k < maxReward + 1; k++) {
                            TsFileResource currTsFileResource = overlappedList.get(k);
                            allReward += currTsFileResource.getTimeIndex().getMaxEndTime() - currTsFileResource.getTimeIndex().getMinStartTime();//这里原本是根据tsfileresource去计算起止时间
                        }
                    }
                    candidateList.add(new long[]{i, j, allReward}); //这里似乎是以下标的方式，记录两两文件的互相之间，reward
                }
            }
            // get the tuple with max reward among candidate list
            long[] maxTuple = new long[]{0, 0, 0L};
            for (long[] tuple : candidateList) { //这里是分析，取出两两之间，ij可能是记录的范围，总之是对比哪一组收益更大
                if (tuple[2] > maxTuple[2]) {
                    maxTuple = tuple;
                }
            }
            List<TsFileResource> selectedFiles = new ArrayList<>();//根据合并受益，选择被合并的候选文件
            for (int i = (int) maxTuple[0]; i < maxTuple[1] + 1; i++) { //把前面收益最大的那一个，对应的ij下标内的文件选择出来
                selectedFiles.add(overlappedList.get(i));//这个或许就是最终选择出来的文件
            }

            // get the select result in order
            selectedFileList.add(currentFile); //把当前层级的文件持续的添加到临时队列selectedFileList当中，只要没满足142行的条件，就一直添加新的进来
            //==========核心逻辑的编码位置=================
            selectedFileSize += currentFile.getTsFileSize();
            LOGGER.debug(
                    "Add tsfile {}, current select file num is {}, size is {}",
                    currentFile,
                    selectedFileList.size(),
                    selectedFileSize);
            // if the file size or file num reach threshold，判断临时队列的数量或者存储空间的大小
            if (selectedFileSize >= targetCompactionFileSize
                    || selectedFileList.size() >= config.getMaxInnerCompactionCandidateFileNum()) {//合并时候候选文件的数量如果为3，那么就合并，原本是30个合并
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

    private List<TsFileResource> calculateOverlappedList(List<TsFileResource> tsFileResources) {
        //从zhanglingzhe0.12版本复现至此，计算重叠度
        List<TsFileResource> overlappedList = new ArrayList<>();
        long time = 0;
        ITimeIndex timeIndex;
        for (int i = tsFileResources.size() - 1; i >= 0; i--) {//遍历每一个资源文件
            TsFileResource tsFileResource = tsFileResources.get(i);
            Set<String> devicesNameInOneTsfie = tsFileResource.getDevices();//获得所有的设备名
            if (!devicesNameInOneTsfie.isEmpty()) {// 原方法tsFileResource.getDeviceToIndexMap().size() > 0
                //寻找有重叠的文件
                timeIndex = tsFileResource.getTimeIndex();
                if (true){//先判断文件是否和查询有交集再说
                    overlappedList.add(tsFileResource);
                }
                time += timeIndex.getMaxEndTime() - timeIndex.getMinStartTime();
                if (time > queryTimeInterval) {//感觉这里应该是计算设备的时间跨度
                    break;
                }
            }
        }
        return overlappedList;
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