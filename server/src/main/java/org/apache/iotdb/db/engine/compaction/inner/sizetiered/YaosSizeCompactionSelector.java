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
import org.apache.iotdb.db.engine.compaction.MLQueryAnalyzerYaos;
import org.apache.iotdb.db.engine.compaction.QueryMonitorYaos;
import org.apache.iotdb.db.engine.compaction.inner.AbstractInnerSpaceCompactionSelector;
import org.apache.iotdb.db.engine.compaction.inner.InnerSpaceCompactionTaskFactory;
import org.apache.iotdb.db.engine.compaction.task.AbstractCompactionTask;
import org.apache.iotdb.db.engine.storagegroup.TsFileManager;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.engine.storagegroup.timeindex.ITimeIndex;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.db.engine.compaction.MLQueryAnalyzerYaos;
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

    private long queryTimeStart= 1706716805000L;//2024-02-01 00:00:05 的 long类型时间戳
    private long queryTimeEnd= 1706716805000L;//2024-02-01 00:00:05 的 long类型时间戳
    private long queryTimeInterval = 604800000L;//86400000 * 7 = 604800000
    //这个参数在zhanglingzhe的代码中单独作为一个静态参数，据说是根据python分析的结果反写回来的

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
        //在文件选择的时候，就不再去分析查询特征了，而是直接调用分析器解析出结果
        //我们在文件选择的时候，就单纯调用分析出来的结果，分析过程在 VSG的方法executeCompaction()里面
        MLQueryAnalyzerYaos MLAnalyzer = MLQueryAnalyzerYaos.getInstance();
        QueryMonitorYaos monitorYaos = QueryMonitorYaos.getInstance();
        //借助QueryMonitorYaos  monitorYaos去分析当前一批查询密度较高的地方
        //double[] Clustered_startTimeSum_InetvalTimeSum_EndTimeSum = monitorYaos.analyzeTheGolableFeatures_UsingNormalCentroid();
        /*
            在开始ML分析之前，就已经借助VSG，在ML训练之前，就已经分析好了当前查询的密集点
            分析出来的当前结果由 Clustered_startTimeSum_InetvalTimeSum_EndTimeSum变量负责保存
         */
        double[] Clustered_startTimeSum_InetvalTimeSum_EndTimeSum = new double[3];
        ArrayList<QueryMonitorYaos.FeatureofOneQuery> queryFeaturesMeanShiftList = QueryMonitorYaos.getQueryFeaturesMeanShiftList();
        System.out.println(queryFeaturesMeanShiftList.get(0));
        Clustered_startTimeSum_InetvalTimeSum_EndTimeSum[0] = queryFeaturesMeanShiftList.get(0).getStartTime();
        Clustered_startTimeSum_InetvalTimeSum_EndTimeSum[1] = queryFeaturesMeanShiftList.get(0).getInterval();
        Clustered_startTimeSum_InetvalTimeSum_EndTimeSum[2] = queryFeaturesMeanShiftList.get(0).getEndTime();

        long predited_Startime = 0;
        long predited_Endtime = 0;
        long Clustered_Startime = 0;
        long Clustered_Endtime = 0;
        ArrayList<QueryMonitorYaos.FeatureofGroupQuery> analyzedGroupFeatruedList = monitorYaos.getAnalyzedGroupsFeatruedList();//获得计算的访问负载特征，查询的起始时间
        //todo 如果近期没有收集到足够的查询负载，那么就按照通常的原始合并去做。会直接跳过ML分析步骤
        if (!analyzedGroupFeatruedList.isEmpty()){//查询数量足够，而且不是空的条件下才去执行ML分析
            LOGGER.info("文件选择器：获取到一批足量查询负载，可以继续ML分析....");
            MLAnalyzer.setQuery(QueryMonitorYaos.getQueryFeaturesGloablList());//把 负载收集器 收集到的结果 发送给 机器学习预测器
            long[] predictedStartimeAndEndTime = null;//调用模型的训练和构建，同时完成输出预测，获得下一个时间可能被访问的
            long[] ClusteredStartimeAndEndTime = null;//调用模型的训练和构建，同时完成输出预测，获得下一个时间可能被访问的

            try {//处理训练模型时发生的异常
                predictedStartimeAndEndTime = MLAnalyzer.TranningAndPredict();//预测即将会被访问到的数据
                //ClusteredStartimeAndEndTime = MLAnalyzer.ClusteringTheCurrentQueryRrange();//汇总当前被访问到的数据，已经改掉了，现在是借助查询分析器去找负载中心

                predited_Startime = predictedStartimeAndEndTime[0];
                predited_Endtime = predictedStartimeAndEndTime[1];//获得预测的下一个时间段，哪些数据可能被访问到

                Clustered_Startime = (long) Clustered_startTimeSum_InetvalTimeSum_EndTimeSum[0];
                Clustered_Endtime = (long) Clustered_startTimeSum_InetvalTimeSum_EndTimeSum[2];//获得预测的下一个时间段，哪些数据可能被访问到
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }else {
            LOGGER.info("文件选择器：没有收集到足够的查询负载");
        }
        //monitorYaos.clearFeatures();//在前面拿到特征之后，清空所有的元素
        //todo 合并收益分析以及预测和聚类方法的联合编程模式，联合多树的合并模式分析
        long cluster_queryTimeStart = Clustered_Startime;//把预测的下一个时间段的可能访问长度给预测出来
        long cluster_queryTimeEnd = Clustered_Endtime;//把预测的下一个时间段的可能访问长度给预测出来
        long cluster_queryTimeInterval = cluster_queryTimeEnd - cluster_queryTimeStart;

        long next_queryTimeStart = predited_Startime * 1000 + 1706700000000L;//把预测的下一个时间段的可能访问长度给预测出来
        long next_queryTimeEnd = predited_Endtime * 1000 + 1706700000000L;//把预测的下一个时间段的可能访问长度给预测出来
        long next_queryTimeInterval = (long) Clustered_startTimeSum_InetvalTimeSum_EndTimeSum[1];

        //重叠分析的结果写回到全局变量里面，如果预测结果和聚类结果没有交集，那么就返回一个候选Pair
        Pair<Long, Long> CandidatelongPair = Overlapanalysis_BetweenClusterAnd(cluster_queryTimeStart, cluster_queryTimeEnd, next_queryTimeStart, next_queryTimeEnd);

        try {
            int maxLevel = searchMaxFileLevel();
            for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
                if (!selectLevelTask_byYaos_V1(currentLevel, taskPriorityQueue)) {
                    //如果在一层中找到了至少一批可以合并的文件，那么就终止，不再判断上面其他层级了
                    //返回的taskPriorityQueue里面会包含一层内的多批次待合并文件资源
                    break;//这里面包含了核心的执行选择合并任务的逻辑,直到当前层里面有就不去遍历下一层了，
                }
            }
            if (CandidatelongPair != null){//两个区间没有交集
                //再搜索一波文件 提交分析，重新以新的分组再次搜索值得合并的文件
                queryTimeStart = CandidatelongPair.left;
                queryTimeEnd = CandidatelongPair.right;
                queryTimeInterval = queryTimeEnd - queryTimeStart;
                for (int currentLevel = 0; currentLevel <= maxLevel; currentLevel++) {
                    if (!selectLevelTask_byYaos_V1(currentLevel, taskPriorityQueue)) {
                        //如果在一层中找到了至少一批可以合并的文件，那么就终止，不再判断上面其他层级了
                        //返回的taskPriorityQueue里面会包含一层内的多批次待合并文件资源
                        break;//这里面包含了核心的执行选择合并任务的逻辑,直到当前层里面有就不去遍历下一层了，
                    }
                }
            }
            while (taskPriorityQueue.size() > 0) { //前面可能遍历得到了好几批，候选文件的集和，这里分别把他们提交成任务
                LOGGER.info("文件选择器：选择了一批文件，但是并不提交合并任务。选择的文件是： ");//即使选择出来了文件，但是先不进行合并任务提交，先阻塞
                System.out.println(taskPriorityQueue.poll().left);
                //createAndSubmitTask(taskPriorityQueue.poll().left); //如果有待合并的文件，那么就就提交这个任务
            }
        } catch (Exception e) {
            LOGGER.error("Exception occurs while selecting files", e);
        }
    }

    /**
     * 用来计算聚合分析的时间区间 ct1 ~ct2 和预测的时间区间 ft3 ft4的重叠性
     * 并将分析的结果写入到全局静态变量 queryTimeStart,queryTimeEnd里面
     * 填充结果写入到全局变量
     */
    private Pair<Long, Long> Overlapanalysis_BetweenClusterAnd(long ct1, long ct2, long ft3, long ft4) {
        long ctInterval = ct2 - ct1;
        long ftInterval = ft4 - ft3;
        long candidateQvStart = 0;
        long candidateQvEnd = 0;
        if (ft3 > ct2){//无交集，直接返回预测的结果
            queryTimeStart = ft3;
            queryTimeEnd  = ft4;
            queryTimeInterval = queryTimeEnd - queryTimeStart;
            candidateQvStart = ct1;
            candidateQvEnd = ct2;
            return new Pair<Long, Long>(candidateQvStart,candidateQvEnd);
        }

        if (ft3 >= ct1 && ft3 <= ct2 && ft4 >= ct2){//普通的交集情况
            queryTimeStart = ct1;
            queryTimeEnd = ft4;
            queryTimeInterval = queryTimeEnd - queryTimeStart;

            return null;
        }
        if (ft3 >= ct1 && ft3 <= ct2 && ft4 < ct2){//聚合包含预测结果
            queryTimeStart = ft3;
            queryTimeEnd  = ct2;
            queryTimeInterval = queryTimeEnd - queryTimeStart;

            return null;
        }
        if (ft3 < ct1 && ft4 < ct2){//预测即将会访问 更老的数据
            //预测的将来访问的数据范围，如果按照越近的数据越容易被访问那么，ft3通常都是比ct1聚合的结果要大
            //认为，预测结果可信度较差，直接把聚合结果作为当前频繁访问项
            queryTimeStart = ft3;
            queryTimeEnd = ct2;
            queryTimeInterval = queryTimeEnd - queryTimeStart;

            return null;
        }else { //没有匹配到任何的区间模式，那么就返回
            queryTimeStart = ct1;
            queryTimeEnd = ct2;
            queryTimeInterval = queryTimeEnd - queryTimeStart;
            return null;

//            candidateQvStart = ft3;
//            candidateQvEnd = ft4;
//            return new Pair<Long, Long>(candidateQvStart,candidateQvEnd);
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
            int level, PriorityQueue<Pair<List<TsFileResource>, Long>> taskPriorityQueue) //文件选择里面的level仅仅是用来筛选本层的文件
            throws IOException {
        boolean shouldContinueToSearch = true;
        long selectedFileSize = 0L;
        long targetCompactionFileSize = config.getTargetCompactionFileSize(); // 1GB的字节

        ////==========核心逻辑的编码位置=================
        // 现在的文件添加和选择方法是按照顺序选择，我们在这个等号的范围内编写自己的文件选择策略
        double mergeSpeed = 10;//这两个写入 合并 速率参数，暂时还不能确定具体数据
        double writeSpeed = 1;//原版本里也没有这个参数
        List<long[]> candidateList = new ArrayList<>();//仅仅记录了下标和位置
        List<TsFileResource> overlappedList = calculateOverlappedList(tsFileResources, level);
        long offsetTime = 0;//overlappedList里面的文件是按照时间先后，新的文件在list的前面 下标号小
        for (int i = 0; i < overlappedList.size(); i++) { //遍历每一个有交叉的文件，在张lz的算法中，只有一个for循环，没有外层的循环，已经改成对应的单个for循环了
            TsFileResource tsFileResource = overlappedList.get(i);
            long mergedTimeInterval = tsFileResource.getTimeIndex().getMaxEndTime() - tsFileResource.getTimeIndex().getMinStartTime();
            //获取当前文件的时间跨度
            //这里原本是根据tsfileresource去计算起止时间,tsFileResource的文件大小是long类型的字节，mergeSpeed是MB，默认是16MB每秒
            long mergeTimeCost = (long) (tsFileResource.getTsFileSize() / mergeSpeed * writeSpeed);//我预计这里应该是用毫秒数，合并耗时，文件大小除以100，注意一下文件大小的单位，long类型，应该是字节数
            if (queryTimeInterval < (mergedTimeInterval + mergeTimeCost + offsetTime)) {//判断文件的跨度还没有超过查询的特征间隔
                continue;
            }
            offsetTime += mergedTimeInterval;
            for (int j = i + 1; j < overlappedList.size(); j++) { //似乎是遍历i后面的每一个文件
                TsFileResource endTsFileResource = overlappedList.get(j);
                mergeTimeCost += endTsFileResource.getTsFileSize() / mergeSpeed * writeSpeed;
                long allReward = 0L;
                int maxReward = j - i; //这个应该是对应文件的数量,在两个文件i，j之间有多少个
                long fullRewardTime = queryTimeInterval - offsetTime - mergedTimeInterval - mergeTimeCost;//可以理解成，有效时间间隔
                allReward += maxReward * fullRewardTime;
                if (allReward > 0) {
                    // calculate not full reward time, from 1 to max_reward, which is active as long as the interval of every file
                    for (int k = 0; k < maxReward + 1; k++) {
                        TsFileResource currTsFileResource = overlappedList.get(k);
                        allReward += currTsFileResource.getTimeIndex().getMaxEndTime() - currTsFileResource.getTimeIndex().getMinStartTime();
                        //这里原本是根据tsfileresource去计算起止时间
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
        TsFileResource currentFile;
        List<TsFileResource> YaosselectedFiles = new ArrayList<>();//根据合并受益，选择被合并的候选文件
        if (!overlappedList.isEmpty()){//如果有文件才执行，以免系统一直报索引溢出的错误
            for (int i = (int) maxTuple[0]; i <= maxTuple[1]; i++) { //把前面收益最大的那一个，对应的ij下标范围内的文件选择出来
                currentFile = overlappedList.get(i);
                YaosselectedFiles.add(overlappedList.get(i));//这个或许就是最终选择出来的文件
                LOGGER.debug("Current File is {}, size is {}", currentFile, currentFile.getTsFileSize());
                selectedFileSize += currentFile.getTsFileSize();
                LOGGER.debug(
                        "Add tsfile {}, current select file num is {}, size is {}",
                        currentFile,
                        YaosselectedFiles.size(),
                        selectedFileSize);
            }
            if (selectedFileSize >= targetCompactionFileSize //注意在选择完文件之后，需要合并的文件数最少得是3
                    || YaosselectedFiles.size() >= config.getMaxInnerCompactionCandidateFileNum()) {//合并时候候选文件的数量如果为3，那么就合并，原本是30个合并
                // submit the task
                if (YaosselectedFiles.size() > 1) {//满足刷写条件之后，就封装这一批文件到任务队列中
                    taskPriorityQueue.add(new Pair<>(new ArrayList<>(YaosselectedFiles), selectedFileSize));
                }
                //selectedFileSize = 0L;//目前算法每次调用时，只封装生成一个合并任务，原本的是在一层中搜索多次，一次搜索封装多个任务
                //因为，只搜索一次，
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

    private List<TsFileResource> calculateOverlappedList(List<TsFileResource> tsFileResources, int level) throws IOException {
        //从zhanglingzhe0.12版本复现至此，计算重叠度
        //后来分析发现，这个不是计算重叠度的，而是判断候选文件列表里，有没有
        List<TsFileResource> overlappedList = new ArrayList<>();
        long time = 0;
        ITimeIndex timeIndex;
        int templevel = 0;//现在编程设计过程中，考虑到level可能得数值为0,因为只有0层的数据才会被频繁查询到
        for (int i = tsFileResources.size() - 1; i >= 0; i--) {//这里是直接获取最新的文件
            //遍历每一个资源文件，看下标的开始索引，是从最后一个开始遍历，默认情况下是先遍历距离当前时间最新的文件
            TsFileResource tsFileResource = tsFileResources.get(i);
            TsFileNameGenerator.TsFileName currentName = //把文件名进行解析成时间戳-版本-合并次数-跨空间次数的格式，判断是否处于当前层级
                    TsFileNameGenerator.getTsFileName(tsFileResource.getTsFile().getName());
            if (currentName.getInnerCompactionCnt() != templevel //如果遍历的时候，跟当前处理的层级不一致，那么就跳过这个文件
                    || tsFileResource.getStatus() != TsFileResourceStatus.CLOSED) { //或者文件不是关闭状态，就跳过
                continue;
            }
            Set<String> devicesNameInOneTsfie = tsFileResource.getDevices();//获得所有的设备名
            if (!devicesNameInOneTsfie.isEmpty()) {// 原方法tsFileResource.getDeviceToIndexMap().size() > 0
                //寻找有重叠的文件，如果这个文件里面有内容
                timeIndex = tsFileResource.getTimeIndex();//这里面记录了文件内每一个序列的起止时间戳
                long maxEndTime = timeIndex.getMaxEndTime();//暂时仅仅以全局的时间去判断，还没精确到具体的一个设备上
                long minStartTime = timeIndex.getMinStartTime();
                if (maxEndTime > queryTimeStart) {
                    /*
                    (1)对于π0只要末尾大于特征起始，就包括进来
                    文件的结束时间在特征的起始时间之后，就给纳入进来
                    因为对于查询过去段时间到现在 current 的 范围查询内，只要大于 特征间隔的就都要
                    对于文件的起始时间大于 current的那些不在范围内的文件还不存在，这一个需要再πn的时候再去考虑；
                    (2)对于πn，需要考虑新生成的文件，和特征区间的包含关系

                    */
                    overlappedList.add(tsFileResource);
                    time += maxEndTime - minStartTime;//感觉这个得放里面，对应一个文件的时间跨度，对于顺序空间来说，每个文件之间是没有重叠的
                    //直接这么加的话，那么应该考虑文件之间在时间上没有重叠，而且连续两个文件在时间戳上是连续的，而不是像我现在，每一个文件内只是一天的段时间
                }
                if (time > queryTimeInterval) {//感觉这里应该是计算设备的时间跨度，我们把时间跨度设置成了7天的间隔；我生成了7个文件，但是这7个文件的真是时间间隔可能并不能超过设定的查询阈值大小
                    break;//这里的判断条件还有待考证，如果每一个文件跨越的时间范围都比较小，累加起来，不会超过一个查询间隔的
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