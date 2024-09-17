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
package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.ReadAmpRecorder;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

public class PageReader implements IPageReader {

  private PageHeader pageHeader;

  protected TSDataType dataType;

  /** decoder for value column */
  protected Decoder valueDecoder;

  /** decoder for time column */
  protected Decoder timeDecoder;

  /** time column in memory */
  protected ByteBuffer timeBuffer;

  /** value column in memory */
  protected ByteBuffer valueBuffer;

  protected Filter filter;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  public PageReader(
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this(null, pageData, dataType, valueDecoder, timeDecoder, filter);
  }

  public PageReader(
      PageHeader pageHeader,
      ByteBuffer pageData,
      TSDataType dataType,
      Decoder valueDecoder,
      Decoder timeDecoder,
      Filter filter) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.timeDecoder = timeDecoder;
    this.filter = filter;
    this.pageHeader = pageHeader;
    splitDataToTimeStampAndValue(pageData);
  }

  /**
   * split pageContent into two stream: time and value
   *
   * @param pageData uncompressed bytes size of time column, time column, value column
   */
  private void splitDataToTimeStampAndValue(ByteBuffer pageData) {
    int timeBufferLength = ReadWriteForEncodingUtils.readUnsignedVarInt(pageData);

    timeBuffer = pageData.slice();
    timeBuffer.limit(timeBufferLength);

    valueBuffer = pageData.slice();
    valueBuffer.position(timeBufferLength);
  }

  /** @return the returned BatchData may be empty, but never be null */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  @Override
  public BatchData getAllSatisfiedPageData(boolean ascending) throws IOException {
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    int RAIndictor = 0;
    if (filter == null || filter.satisfy(getStatistics())) {
      while (timeDecoder.hasNext(timeBuffer)) {
        long timestamp = timeDecoder.readLong(timeBuffer);
        RAIndictor++;
        switch (dataType) {
          case BOOLEAN:
            boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBoolean))) {
              pageData.putBoolean(timestamp, aBoolean);
            }
            break;
          case INT32:
            int anInt = valueDecoder.readInt(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
              pageData.putInt(timestamp, anInt);
            }
            break;
          case INT64:
            long aLong = valueDecoder.readLong(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
              pageData.putLong(timestamp, aLong);
            }
            break;
          case FLOAT:
            float aFloat = valueDecoder.readFloat(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
              pageData.putFloat(timestamp, aFloat);
            }
            break;
          case DOUBLE:
            double aDouble = valueDecoder.readDouble(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
              pageData.putDouble(timestamp, aDouble);
            }
            break;
          case TEXT:
            Binary aBinary = valueDecoder.readBinary(valueBuffer);
            if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
              pageData.putBinary(timestamp, aBinary);
            }
            break;
          default:
            throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
      }
    }
    int NeedpageDataCount = pageData.getCount();
    //ReadAmpRecorder RArecorder = ReadAmpRecorder.getInstance();//注意，这里额外引入了一个maven依赖到server包中，可以参见git提交记录
    //RArecorder.RecordOneSeries(NeedpageDataCount, RAIndictor);
// 指定要写入的文件路径
    String filePath = "RAoutput_CompactedFiles.csv";
    // 使用try-with-resources语句自动关闭资源
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
      // 将文本写入文件，true表示追加模式
//      writer.write("PageReader类里，需要被返回的行数：" + NeedpageDataCount
//              + " ，实际读取的行数:" + RAIndictor + " ，读点数放大比例： " + (double)RAIndictor / (double)NeedpageDataCount);
      writer.write(NeedpageDataCount + "," + RAIndictor + "\n");
//      System.out.println("PageReader类里，需要被返回的行数：" + NeedpageDataCount
//              + " ，实际读取的行数:" + RAIndictor + " ，读点数放大比例： " + (double)RAIndictor / (double)NeedpageDataCount);
      // 添加换行符，以便于每次写入的内容不会在同一行
      //writer.newLine();
    } catch (IOException e) {
      // 处理可能的异常
      e.printStackTrace();
    }
    return pageData.flip();
  }

  @Override
  public Statistics getStatistics() {
    return pageHeader.getStatistics();
  }

  @Override
  public void setFilter(Filter filter) {
    if (this.filter == null) {
      this.filter = filter;
    } else {
      this.filter = new AndFilter(this.filter, filter);
    }
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  @Override
  public boolean isModified() {
    return pageHeader.isModified();
  }

  protected boolean isDeleted(long timestamp) {
    while (deleteIntervalList != null && deleteCursor < deleteIntervalList.size()) {
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) {
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax() < timestamp) {
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }
}
