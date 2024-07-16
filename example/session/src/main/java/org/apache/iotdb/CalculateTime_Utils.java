package org.apache.iotdb;

import org.apache.iotdb.tsfile.utils.Pair;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName : CalculateTime_Utils
 * Package :
 * Description : 用来方便我去计算long时间戳和普通日期之间的换算关系
 *
 * @Create :2024/7/11-16:35
 */
public class CalculateTime_Utils {

    public static void main(String[] args) throws ParseException {
        System.out.println(LongToData(1707667500000L, false));
        System.out.println(DateToLongTimestamp("2024-02-01 00:00:05"));
        System.out.println(DateToLongTimestamp("2024-02-01 00:00:05.123"));
        Pair<String, String> stringStringPair = parseAndGetTimeRange_Q2("[(time > 1722010210 && time < 1220572242 && 2313131)]");
        System.out.println(stringStringPair.toString());

//        parseAndGetTimeRange_Q3("(value > -5.0 && (time >= 1707321900000 && time <= 1707322150000))");
//        parseAndGetTimeRange_Q3("(time >= 111 && time <= 2222))");
//        parseAndGetTimeRange_Q3("[(time > 1722010210 && time < 1220572242 && 2313131)]");
        String input = "(value > sdb -5.0 gh, &&kj  (time >= 1707321900000 && time < 1707322150000))";
        Pair<String, String> stringStringPair1 = parseAndGetTimeRange_Q3(input);
        System.out.println(stringStringPair1);
        // 测试不同的比较符号
        String input2 = "[(time > 111111 && time < 222222 && 2313133121)]";
        Pair<String, String> stringStringPair2 = parseAndGetTimeRange_Q3(input2);
        System.out.println(stringStringPair2);

    }

    public static String LongToData(long timestamp, boolean ms) {
        SimpleDateFormat dateFormat;
        if (ms) {//如果传入为true，就启用详细的毫秒数
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        } else {//如果传入为false，就启用详细的毫秒数
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        return dateFormat.format(new Date(timestamp));
    }

    public static long DateToLongTimestamp(String dateString) throws ParseException, ParseException {
        SimpleDateFormat dateFormat = null;
        if (dateString.length() < 20) {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        } else {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
        Date date = dateFormat.parse(dateString);
        return date.getTime();
    }

    private static Pair<String, String> parseAndGetTimeRange_Q2(String sinput) {
        Pattern pattern = Pattern.compile("\\d+");
        try {
            Matcher matcher = pattern.matcher(sinput);
            List<String> numbers = new ArrayList<>();
            while (matcher.find()) {
                numbers.add(matcher.group());
            }
            return new Pair<String, String>(numbers.get(0), numbers.get(1));
        } catch (Exception e) {
            System.out.println("解析Q2时发生异常，请查看！");
            return new Pair<String, String>("0", "0");
        }
    }

    private static Pair<String, String> parseAndGetTimeRange_Q3(String input) {
        // 正则表达式匹配 "time" 后跟任意比较符号和数字
        String regex = "time\\s+([<>!=]=?)\\s+(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        try {
            Matcher matcher = pattern.matcher(input);
            List<String> timeValues = new ArrayList<>();
            while (matcher.find()) {
                timeValues.add(matcher.group(2));
            }
            // 打印所有找到的时间值
            if (!timeValues.isEmpty()) {
                return new Pair<>(timeValues.get(0), timeValues.get(1));
            } else {
                System.out.println("解析Q3过滤字符串时发生异常，请查看！");
                return new Pair<>("0", "0");
            }
        }catch (Exception e){
            System.out.println("解析Q3过滤字符串时发生异常，请查看！");
            return new Pair<String,String>("0","0");
        }
    }
}
