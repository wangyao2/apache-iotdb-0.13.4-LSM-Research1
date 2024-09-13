package org.apache.iotdb.tsfile.read.reader.page;

import java.util.HashMap;

/**
 * ClassName : ReadAmpRecorder
 * Package :
 * Description :读放大记录器，用来记录系统数据的读放大情况，我感觉应该以每一个查询为单位，记录一条查询的读放大一个总的list，记录读放大
 * 考虑要不要把这个记录，归为查询负载收集器去管理
 * 记录这一条查询涉及到的所有点数list，还有实际生成的点数list
 *
 * @Create :2024/9/13-20:12
 */
public class ReadAmpRecorder {
    HashMap SeriesRecored= new HashMap();//SeriesRecored中的Key是每一条序列；Value是一个list，记录了访问过程中的读放大状态


}
