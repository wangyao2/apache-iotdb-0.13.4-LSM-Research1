package org.apache.iotdb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
/**
 * ClassName : CalculateTime_Utils
 * Package :
 * Description : 用来方便我去计算long时间戳和普通日期之间的换算关系
 *
 * @Create :2024/7/11-16:35
 */
public class CalculateTime_Utils {

    public static void main(String[] args) throws ParseException {
        System.out.println(LongToData(29994735,false));
        System.out.println(DateToLongTimestamp("2024-02-01 00:00:05"));
        System.out.println(DateToLongTimestamp("2024-02-01 00:00:05.123"));
    }

    public static String LongToData(long timestamp, boolean ms) {
        SimpleDateFormat dateFormat;
        if (ms){//如果传入为true，就启用详细的毫秒数
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }else {//如果传入为false，就启用详细的毫秒数
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        return dateFormat.format(new Date(timestamp));
    }

    public static long DateToLongTimestamp(String dateString) throws ParseException, ParseException {
        SimpleDateFormat dateFormat = null;
        if (dateString.length() < 20){
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }else {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
        Date date = dateFormat.parse(dateString);
        return date.getTime();
    }
}
