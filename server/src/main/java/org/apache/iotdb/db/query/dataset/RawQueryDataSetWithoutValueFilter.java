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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.concurrent.WrappedRunnable;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.AlignedPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.control.QueryTimeManager;
import org.apache.iotdb.db.query.pool.RawQueryReadTaskPoolManager;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.ExceptionBatchData;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.SignalBatchData;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RawQueryDataSetWithoutValueFilter extends QueryDataSet
    implements DirectAlignByTimeDataSet {

  protected class ReadTask extends WrappedRunnable {

    private final ManagedSeriesReader reader; //主要用来读取单个时间序列的数据//每一个读任务都持有一个IBatchReader，通过IBatchReader再去进一步持有SeriesReader，SeriesReader里面持有了带读取的资源
    private final String pathName;
    private final BlockingQueue<BatchData> blockingQueue; //ReadTask里面的哪个blocking就单是一个链表，记录当前序列的数据,这个链表应该会被加入到主类里面？
    private int[] batchDataLengthList;
    private final int seriesIndex;
    private final int fetchLimit;

    public ReadTask(
        ManagedSeriesReader reader,
        BlockingQueue<BatchData> blockingQueue,
        String pathName,
        int[] batchDataLengthList,
        int seriesIndex,
        int fetchLimit) {
      this.reader = reader;
      this.blockingQueue = blockingQueue;
      this.pathName = pathName;
      this.batchDataLengthList = batchDataLengthList;
      this.seriesIndex = seriesIndex;
      this.fetchLimit = fetchLimit;
    }

    @Override
    public void runMayThrow() {
      try {
        // check the status of mainThread before next reading
        // 1. Main thread quits because of timeout
        // 2. Main thread quits because of getting enough fetchSize result
        if (!QueryTimeManager.checkQueryAlive(queryId)) {
          return;
        }

        synchronized (reader) {
          // if the task is submitted, there must be free space in the queue
          // so here we don't need to check whether the queue has free space
          // the reader has next batch
          while (reader.hasNextBatch()) { // 在这个hasNextBatch（）方法里面就去尝试读取内部是数据
            BatchData batchData = reader.nextBatch();
            //System.out.println("batchData的行数，RawQueryDataSetWithoutValueFilter：" + batchData.getCount());
            // iterate until we get first batch data with valid value
            if (batchData.isEmpty()) {
              continue;
            }
            blockingQueue.put(batchData); //根据一天的分析，这一个blockingQueue就对应了主类blockingQueueArray[]里面某一个索引上的

            // has limit clause
            if (batchDataLengthList != null) {
              batchDataLengthList[seriesIndex] += batchData.length();
              if (batchDataLengthList[seriesIndex] >= fetchLimit) {
                // the queue has enough space to hold SignalBatchData, just break the while loop
                if (blockingQueue.remainingCapacity() > 0) {
                  break;
                } else { // otherwise, exit without putting SignalBatchData, main thread will submit
                  // a new task again, then it will put SignalBatchData successfully
                  reader.setManagedByQueryManager(false);
                  return;
                }
              }
            }
            // if the queue also has free space, just submit another itself
            if (blockingQueue.remainingCapacity() > 0) {
              TASK_POOL_MANAGER.submit(this);
            }
            // the queue has no more space
            // remove itself from the QueryTaskPoolManager
            else {
              reader.setManagedByQueryManager(false);
            }
            return;
          }
          // there are no batch data left in this reader
          // put the signal batch data into queue
          blockingQueue.put(SignalBatchData.getInstance());
          // set the hasRemaining field in reader to false
          // tell the Consumer not to submit another task for this reader any more
          reader.setHasRemaining(false);
          // remove itself from the QueryTaskPoolManager
          reader.setManagedByQueryManager(false);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while putting into the blocking queue: ", e);
        Thread.currentThread().interrupt();
        reader.setHasRemaining(false);
      } catch (IOException e) {
        putExceptionBatchData(
            e,
            String.format(
                "Something gets wrong while reading from the series reader %s: ", pathName));
      } catch (Throwable e) {
        putExceptionBatchData(e, "Something gets wrong: ");
      }
    }

    private void putExceptionBatchData(Throwable e, String logMessage) {
      try {
        LOGGER.error(logMessage, e);
        reader.setHasRemaining(false);
        blockingQueue.put(new ExceptionBatchData(e));
      } catch (InterruptedException ex) {
        LOGGER.error("Interrupted while putting ExceptionBatchData into the blocking queue: ", ex);
        Thread.currentThread().interrupt();
      }
    }
  }

  protected List<ManagedSeriesReader> seriesReaderList;

  protected TimeSelector timeHeap;

  // Blocking queue list for each batch reader
  protected final BlockingQueue<BatchData>[] blockingQueueArray; //这里是一个链表的数组，有一条被查询的序列对应一个位置，ReadTask里面的哪个blocking就单是一个链表，记录当前序列的数据

  // indicate that there is no more batch data in the corresponding queue
  // in case that the consumer thread is blocked on the queue and won't get runnable any more
  // this field is not same as the `hasRemaining` in SeriesReaderWithoutValueFilter
  // even though the `hasRemaining` in SeriesReaderWithoutValueFilter is false
  // noMoreDataInQueue can still be true
  // its usage is to tell the consumer thread not to call the take() method.
  protected boolean[] noMoreDataInQueueArray;

  protected BatchData[] cachedBatchDataArray; //就是一维数组，临时存储当前序列里面的所有batchdata

  protected int[] batchDataLengthList;

  private int bufferNum;

  // capacity for blocking queue
  private static final int BLOCKING_QUEUE_CAPACITY =
      IoTDBDescriptor.getInstance().getConfig().getRawQueryBlockingQueueCapacity();

  private final long queryId;

  // this field record the original value of offset clause, won't change during the query execution
  protected final int originalRowOffset;

  private static final RawQueryReadTaskPoolManager TASK_POOL_MANAGER =
      RawQueryReadTaskPoolManager.getInstance();

  private static final Logger LOGGER =
      LoggerFactory.getLogger(RawQueryDataSetWithoutValueFilter.class);

  /**
   * constructor of EngineDataSetWithoutValueFilter.
   *
   * @param readers readers in List(IPointReader) structure
   */
  public RawQueryDataSetWithoutValueFilter(
      long queryId, RawDataQueryPlan queryPlan, List<ManagedSeriesReader> readers)
      throws IOException, InterruptedException {
    super(
        new ArrayList<>(queryPlan.getDeduplicatedPaths()),
        queryPlan.getDeduplicatedDataTypes(),
        queryPlan.isAscending());
    this.rowLimit = queryPlan.getRowLimit();
    this.originalRowOffset = queryPlan.getRowOffset();
    this.rowOffset = queryPlan.getRowOffset();
    this.withoutAnyNull = queryPlan.isWithoutAnyNull();
    this.withoutAllNull = queryPlan.isWithoutAllNull();
    if (rowLimit != 0 && !withoutAllNull && !withoutAnyNull) {
      batchDataLengthList = new int[readers.size()];
    }

    this.queryId = queryId;
    this.seriesReaderList = readers;
    blockingQueueArray = new BlockingQueue[readers.size()];
    for (int i = 0; i < seriesReaderList.size(); i++) {
      blockingQueueArray[i] = new LinkedBlockingQueue<>(BLOCKING_QUEUE_CAPACITY);
    }
    cachedBatchDataArray = new BatchData[readers.size()];
    noMoreDataInQueueArray = new boolean[readers.size()];
    bufferNum = 0;
    for (PartialPath path : queryPlan.getDeduplicatedPaths()) {
      if (path instanceof AlignedPath) {
        bufferNum += ((AlignedPath) path).getMeasurementList().size();
      } else {
        bufferNum += 1;
      }
    }
    init();
  }

  /**
   * Dummy dataSet for redirect query.
   *
   * @param queryId queryId for the query.
   */
  public RawQueryDataSetWithoutValueFilter(long queryId) {
    this.queryId = queryId;
    this.originalRowOffset = 0;
    blockingQueueArray = new BlockingQueue[0];
    timeHeap = new TimeSelector(0, ascending);
  }

  private void init() throws IOException, InterruptedException {
    timeHeap = new TimeSelector(seriesReaderList.size() << 1, ascending);
    for (int i = 0; i < seriesReaderList.size(); i++) {//遍历每一个传感器阅读器，针对每一条时间序列，都创建一个查询任务
      ManagedSeriesReader reader = seriesReaderList.get(i);
      reader.setHasRemaining(true);
      reader.setManagedByQueryManager(true);
      TASK_POOL_MANAGER.submit(generateReadTaskForGivenReader(reader, i)); //为每一个时间序列构建生产者任务,这个索引编号i还对应了blockingQueuArray数组里面的位置
    }
    for (int i = 0; i < seriesReaderList.size(); i++) { //// 初始化最小堆，填充每个时间序列对应的缓存
      // check the interrupted status of query before taking next batch
      QueryTimeManager.checkQueryAlive(queryId);
      fillCache(i);  // 调用 fillCache(int) 方法填充缓存，序号i对应了每一条的时间序列，每一次调用都是只拿序列里面的一块数据去处理，没看到里面有循环的成分
      // try to put the next timestamp into the heap // 尝试将每个时间序列的当前最小时间戳放进堆中
      if (cachedBatchDataArray[i] != null && cachedBatchDataArray[i].hasCurrent()) {
        long time = cachedBatchDataArray[i].currentTime();
        timeHeap.add(time);
      }
    }
  }

  protected ReadTask generateReadTaskForGivenReader(ManagedSeriesReader reader, int seriesIndex) {
    return new ReadTask( //创建一个阅读任务出来
        reader,
        blockingQueueArray[seriesIndex], //blocking里面的内容是batchdata，也是根据待查序列的数量定出来的,每一个序列对应一个号
        paths.get(seriesIndex).getFullPath(),
        batchDataLengthList,
        seriesIndex, // 序列对应了自己的索引号
        rowLimit + originalRowOffset);
  }

  /**
   * for RPC in RawData query between client and server fill time buffer, value buffers and bitmap
   * buffers
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) //这个是提供给外面的TSServiceImpl ，rpc的服务类调用的工具接口
      throws IOException, InterruptedException {
    int seriesNum = seriesReaderList.size(); //将每个时间戳的数据做对齐，并将结果组装成TSQueryDataSet返回
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();

    PublicBAOS timeBAOS = new PublicBAOS();

    PublicBAOS[] valueBAOSList = new PublicBAOS[bufferNum];
    PublicBAOS[] bitmapBAOSList = new PublicBAOS[bufferNum];

    for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {
      valueBAOSList[bufferIndex] = new PublicBAOS();
      bitmapBAOSList[bufferIndex] = new PublicBAOS();
    }

    // used to record a bitmap for every 8 row records
    int[] currentBitmapList = new int[bufferNum];
    int rowCount = 0;
    while (rowCount < fetchSize) {

      if ((rowLimit > 0 && alreadyReturnedRowNum >= rowLimit) || timeHeap.isEmpty()) {
        break;
      }

      long minTime = timeHeap.pollFirst();

      if ((withoutAnyNull || withoutAllNull) && filterRowRecord(seriesNum, minTime)) {
        continue;
      }

      if (rowOffset == 0) {
        timeBAOS.write(BytesUtils.longToBytes(minTime));
      }

      for (int seriesIndex = 0, bufferIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
          //处理每一条序列，seriesIndex是当前处理的序列编号号，seriesNum是序列总数，在前面已经显示了
        if (cachedBatchDataArray[seriesIndex] == null
            || !cachedBatchDataArray[seriesIndex].hasCurrent()
            || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
          // current batch is empty or does not have value at minTime
          if (rowOffset == 0) {
            if (paths.get(seriesIndex) instanceof AlignedPath) {
              for (int i = 0;
                  i < ((AlignedPath) paths.get(seriesIndex)).getMeasurementList().size();
                  i++) {
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1);
                bufferIndex++;
              }
            } else {
              currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1);
              bufferIndex++;
            }
          }
        } else {
          // current batch has value at minTime, consume current value
          if (rowOffset == 0) {
            TSDataType type = cachedBatchDataArray[seriesIndex].getDataType();
            switch (type) {
              case INT32:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                int intValue = cachedBatchDataArray[seriesIndex].getInt();
                if (encoder != null && encoder.needEncode(minTime)) {
                  intValue = encoder.encodeInt(intValue, minTime);
                }
                ReadWriteIOUtils.write(intValue, valueBAOSList[bufferIndex]);
                bufferIndex++;
                break;
              case INT64:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                long longValue = cachedBatchDataArray[seriesIndex].getLong();
                if (encoder != null && encoder.needEncode(minTime)) {
                  longValue = encoder.encodeLong(longValue, minTime);
                }
                ReadWriteIOUtils.write(longValue, valueBAOSList[bufferIndex]);
                bufferIndex++;
                break;
              case FLOAT:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                float floatValue = cachedBatchDataArray[seriesIndex].getFloat();
                if (encoder != null && encoder.needEncode(minTime)) {
                  floatValue = encoder.encodeFloat(floatValue, minTime);
                }
                ReadWriteIOUtils.write(floatValue, valueBAOSList[bufferIndex]);
                bufferIndex++;
                break;
              case DOUBLE:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                double doubleValue = cachedBatchDataArray[seriesIndex].getDouble();
                if (encoder != null && encoder.needEncode(minTime)) {
                  doubleValue = encoder.encodeDouble(doubleValue, minTime);
                }
                ReadWriteIOUtils.write(doubleValue, valueBAOSList[bufferIndex]);
                bufferIndex++;
                break;
              case BOOLEAN:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                ReadWriteIOUtils.write(
                    cachedBatchDataArray[seriesIndex].getBoolean(), valueBAOSList[bufferIndex]);
                bufferIndex++;
                break;
              case TEXT:
                currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                ReadWriteIOUtils.write(
                    cachedBatchDataArray[seriesIndex].getBinary(), valueBAOSList[bufferIndex]);
                bufferIndex++;
                break;
              case VECTOR:
                for (TsPrimitiveType primitiveVal : cachedBatchDataArray[seriesIndex].getVector()) {
                  if (primitiveVal == null) {
                    currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1);
                    bufferIndex++;
                    continue;
                  }
                  currentBitmapList[bufferIndex] = (currentBitmapList[bufferIndex] << 1) | FLAG;
                  switch (primitiveVal.getDataType()) {
                    case INT32:
                      int intVal = primitiveVal.getInt();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        intVal = encoder.encodeInt(intVal, minTime);
                      }
                      ReadWriteIOUtils.write(intVal, valueBAOSList[bufferIndex]);
                      break;
                    case INT64:
                      long longVal = primitiveVal.getLong();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        longVal = encoder.encodeLong(longVal, minTime);
                      }
                      ReadWriteIOUtils.write(longVal, valueBAOSList[bufferIndex]);
                      break;
                    case FLOAT:
                      float floatVal = primitiveVal.getFloat();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        floatVal = encoder.encodeFloat(floatVal, minTime);
                      }
                      ReadWriteIOUtils.write(floatVal, valueBAOSList[bufferIndex]);
                      break;
                    case DOUBLE:
                      double doubleVal = primitiveVal.getDouble();
                      if (encoder != null && encoder.needEncode(minTime)) {
                        doubleVal = encoder.encodeDouble(doubleVal, minTime);
                      }
                      ReadWriteIOUtils.write(doubleVal, valueBAOSList[bufferIndex]);
                      break;
                    case BOOLEAN:
                      ReadWriteIOUtils.write(primitiveVal.getBoolean(), valueBAOSList[bufferIndex]);
                      break;
                    case TEXT:
                      ReadWriteIOUtils.write(primitiveVal.getBinary(), valueBAOSList[bufferIndex]);
                      break;
                    default:
                      throw new UnSupportedDataTypeException(
                          String.format("Data type %s is not supported.", type));
                  }
                  bufferIndex++;
                }
                break;
              default:
                throw new UnSupportedDataTypeException(
                    String.format("Data type %s is not supported.", type));
            }
          }

          prepareForNext(seriesIndex); //在这里再一次去读取调用fillCache
        }
      }

      if (rowOffset == 0) {
        rowCount++;
        if (rowCount % 8 == 0) {
          for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {
            ReadWriteIOUtils.write(
                (byte) currentBitmapList[bufferIndex], bitmapBAOSList[bufferIndex]);
            // we should clear the bitmap every 8 row record
            currentBitmapList[bufferIndex] = 0;
          }
        }
        if (rowLimit > 0) {
          alreadyReturnedRowNum++;
        }
      } else {
        rowOffset--;
      }
    }

    /*
     * feed the bitmap with remaining 0 in the right
     * if current bitmap is 00011111 and remaining is 3, after feeding the bitmap is 11111000
     */
    if (rowCount > 0) {
      int remaining = rowCount % 8;
      if (remaining != 0) {
        for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {
          ReadWriteIOUtils.write(
              (byte) (currentBitmapList[bufferIndex] << (8 - remaining)),
              bitmapBAOSList[bufferIndex]);
        }
      }
    }

    // set time buffer
    ByteBuffer timeBuffer = ByteBuffer.allocate(timeBAOS.size());
    timeBuffer.put(timeBAOS.getBuf(), 0, timeBAOS.size());
    timeBuffer.flip();
    tsQueryDataSet.setTime(timeBuffer);

    List<ByteBuffer> valueBufferList = new ArrayList<>();
    List<ByteBuffer> bitmapBufferList = new ArrayList<>();

    for (int bufferIndex = 0; bufferIndex < bufferNum; bufferIndex++) {

      // add value buffer of current series
      putPBOSToBuffer(valueBAOSList, valueBufferList, bufferIndex);

      // add bitmap buffer of current series
      putPBOSToBuffer(bitmapBAOSList, bitmapBufferList, bufferIndex);
    }

    // set value buffers and bitmap buffers
    tsQueryDataSet.setValueList(valueBufferList);
    tsQueryDataSet.setBitmapList(bitmapBufferList);

    return tsQueryDataSet;
  }

  /** if columns in the row record match the condition of null value filter, we filter it. */
  private boolean filterRowRecord(int seriesNum, long minTime)
      throws IOException, InterruptedException {
    boolean hasNull = false, isAllNull = true;
    // because `cachedBatchDataArray[seriesIndex]` may be TSDataType.VECTOR type
    // so seriesIndex may not be corresponding to `withoutNullColumnsIndex`
    // we need the `index` to record
    int index = 0;
    for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
      if (withoutNullColumnsIndex != null && !withoutNullColumnsIndex.contains(index)) {
        index++;
        continue;
      }

      if (cachedBatchDataArray[seriesIndex] == null
          || !cachedBatchDataArray[seriesIndex].hasCurrent()
          || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
        index++;
        hasNull = true;
      } else {
        if (TSDataType.VECTOR == cachedBatchDataArray[seriesIndex].getDataType()) {
          boolean nullFlag = false;
          for (TsPrimitiveType primitiveVal : cachedBatchDataArray[seriesIndex].getVector()) {
            if (withoutNullColumnsIndex != null && !withoutNullColumnsIndex.contains(index)) {
              index++;
              continue;
            }
            if (primitiveVal == null) {
              hasNull = true;
              nullFlag = true;
            } else {
              isAllNull = false;
            }
            index++;
          }

          if (!nullFlag) {
            isAllNull = false;
            if (isWithoutAllNull()) {
              break;
            }
          }
        } else {
          index++;
          isAllNull = false;
        }
      }
      if (hasNull && isWithoutAnyNull()) {
        break;
      }

      if (!hasNull) {
        isAllNull = false;
      }

      if (!isAllNull && isWithoutAllNull()) {
        break;
      }
    }
    if (hasNull && isWithoutAnyNull()) {
      for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
        if (cachedBatchDataArray[seriesIndex] != null
            && cachedBatchDataArray[seriesIndex].hasCurrent()
            && cachedBatchDataArray[seriesIndex].currentTime() == minTime) {
          prepareForNext(seriesIndex);
        }
      }
      return true;
    }

    if (isAllNull && isWithoutAllNull()) {
      if (withoutNullColumnsIndex != null) {
        for (int seriesIndex = 0; seriesIndex < seriesNum; seriesIndex++) {
          if (cachedBatchDataArray[seriesIndex] != null
              && cachedBatchDataArray[seriesIndex].hasCurrent()
              && cachedBatchDataArray[seriesIndex].currentTime() == minTime) {
            prepareForNext(seriesIndex);
          }
        }
      }
      return true;
    }
    return false;
  }

  private void prepareForNext(int seriesIndex) throws IOException, InterruptedException {
    // move next
    cachedBatchDataArray[seriesIndex].next();

    // get next batch if current batch is empty and still have remaining batch data in queue
    if (!cachedBatchDataArray[seriesIndex].hasCurrent() && !noMoreDataInQueueArray[seriesIndex]) {
      fillCache(seriesIndex);
    }

    // try to put the next timestamp into the heap
    if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
      timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
    }
  }

  protected void fillCache(int seriesIndex) throws IOException, InterruptedException { //一条一条的处理每一个序列，该方法负责从阻塞队列中取出数据，并填充缓存
    BatchData batchData = blockingQueueArray[seriesIndex].take();  // 从阻塞队列中拿数据，如果没有数据，则会阻塞等待队列中有数据
    // no more batch data in this time series queue    int batchDatacount = batchData.getCount();
    if (batchData instanceof SignalBatchData) { // 如果是一个信号 SingalBatchData，则将相应时间序列的 oMoreDataInQueue 置为 false ，信号的作用是告诉调用者后面没有更多的batch数据了
      noMoreDataInQueueArray[seriesIndex] = true;
    } else if (batchData instanceof ExceptionBatchData) { // // 当生产者线程发生异常时释放查询资源
      // exception happened in producer thread
      ExceptionBatchData exceptionBatchData = (ExceptionBatchData) batchData;
      LOGGER.error("exception happened in producer thread", exceptionBatchData.getThrowable());
      if (exceptionBatchData.getThrowable() instanceof IOException) {
        throw (IOException) exceptionBatchData.getThrowable();
      } else if (exceptionBatchData.getThrowable() instanceof RuntimeException) {
        throw (RuntimeException) exceptionBatchData.getThrowable();
      } else {
        throw new RuntimeException("some other unknown errors!");
      }

    } else { // there are more batch data in this time series queue
      cachedBatchDataArray[seriesIndex] = batchData;  // 从BlockingQueue<BatchData> 里面去除第一个batchdata，将取出的 BatchData 放进 cachedBatchDataArray 缓存起来
      //封装的这个cachedBatchDataArray是为了方便外面的RPC实现类TSServiceImpl里面直接读取调用
      synchronized (seriesReaderList.get(seriesIndex)) {
        // we only need to judge whether to submit another task when the queue is not full
        if (blockingQueueArray[seriesIndex].remainingCapacity() > 0) { // 看了一下最大容量是5，在新建ValueFilter的时候能看到//只有当阻塞队列不满的时候，我们才需要判断是不是需要提交生产者任务，这里也保证了生产者任务会被提交，当且仅当阻塞队列不满
          ManagedSeriesReader reader = seriesReaderList.get(seriesIndex);
          // if the reader isn't being managed and still has more data,
          // that means this read task leave the pool before because the queue has no more space
          // now we should submit it again
          if (!reader.isManagedByQueryManager() && reader.hasRemaining()) {
            reader.setManagedByQueryManager(true);
            TASK_POOL_MANAGER.submit(generateReadTaskForGivenReader(reader, seriesIndex));
          }
        }
      }
    }
  }

  private void putPBOSToBuffer(
      PublicBAOS[] bitmapBAOSList, List<ByteBuffer> bitmapBufferList, int tsIndex) {
    ByteBuffer bitmapBuffer = ByteBuffer.allocate(bitmapBAOSList[tsIndex].size());
    bitmapBuffer.put(bitmapBAOSList[tsIndex].getBuf(), 0, bitmapBAOSList[tsIndex].size());
    bitmapBuffer.flip();
    bitmapBufferList.add(bitmapBuffer);
  }

  /** for spark/hadoop/hive integration and test */
  @Override
  public boolean hasNextWithoutConstraint() {
    return !timeHeap.isEmpty();
  }

  /** for spark/hadoop/hive integration and test */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeap.pollFirst();
    RowRecord record = new RowRecord(minTime);

    int seriesNumber = seriesReaderList.size();
    for (int seriesIndex = 0; seriesIndex < seriesNumber; seriesIndex++) {
      if (cachedBatchDataArray[seriesIndex] == null
          || !cachedBatchDataArray[seriesIndex].hasCurrent()
          || cachedBatchDataArray[seriesIndex].currentTime() != minTime) {
        if (paths.get(seriesIndex) instanceof AlignedPath) {
          for (int i = 0;
              i < ((AlignedPath) paths.get(seriesIndex)).getMeasurementList().size();
              i++) {
            record.addField(null);
          }
        } else {
          record.addField(null);
        }
      } else {
        TSDataType dataType = dataTypes.get(seriesIndex);
        if (dataType == TSDataType.VECTOR) {
          for (TsPrimitiveType primitiveVal : cachedBatchDataArray[seriesIndex].getVector()) {
            if (primitiveVal == null) {
              record.addField(null);
            } else {
              record.addField(primitiveVal.getValue(), primitiveVal.getDataType());
            }
          }
        } else {
          record.addField(cachedBatchDataArray[seriesIndex].currentValue(), dataType);
        }
        cacheNext(seriesIndex);
      }
    }

    return record;
  }

  protected void cacheNext(int seriesIndex) throws IOException {
    // move next
    cachedBatchDataArray[seriesIndex].next();

    // check the interrupted status of query before taking next batch
    QueryTimeManager.checkQueryAlive(queryId);

    // get next batch if current batch is empty and still have remaining batch data in queue
    if (!cachedBatchDataArray[seriesIndex].hasCurrent() && !noMoreDataInQueueArray[seriesIndex]) {
      try {
        fillCache(seriesIndex);
      } catch (InterruptedException e) {
        LOGGER.error("Interrupted while taking from the blocking queue: ", e);
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        LOGGER.error("Got IOException", e);
        throw e;
      }
    }

    // try to put the next timestamp into the heap
    if (cachedBatchDataArray[seriesIndex].hasCurrent()) {
      timeHeap.add(cachedBatchDataArray[seriesIndex].currentTime());
    }
  }
}
