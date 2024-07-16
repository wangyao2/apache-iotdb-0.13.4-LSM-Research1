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

        Pair<String, String> stringStringPair3 = parseAndGetTimeRange_Q2("[time >= 1707321900000]");
        System.out.println(stringStringPair3);
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
        // 正则表达式匹配时间范围
        Pattern pattern = Pattern.compile("\\d+");//匹配字符串里面所有的整数，正好可以与Q2形式匹配
        try {
            Matcher matcher = pattern.matcher(sinput);
            List<String> numbers = new ArrayList<>();
            while (matcher.find()) {
                numbers.add(matcher.group());
            }
            if (numbers.size() == 1){
                return new Pair<String,String>(numbers.get(0),null);
            }
            return new Pair<String,String>(numbers.get(0),numbers.get(1));
        }catch (Exception e){
            System.out.println("解析Q2过滤字符串时发生异常，请查看！");
            return new Pair<String,String>("0","0");
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
    public static void extractTimeValues(String input) {
        // 正则表达式匹配 "time" 后跟任意比较符号和数字
        String regex = "time\\s+([<>!=]=?)\\s+(\\d+)";
        // 注意：这个正则表达式会匹配每个单独的时间比较，如果需要匹配成对的时间（如 >= 和 <=），则需要更复杂的逻辑

        // 如果你需要匹配成对的（比如同时匹配 >= 和 <=），你可能需要使用多次匹配并检查上下文
        // 但为了简化，这里我们只匹配单个时间比较

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        List<Long> timeValues = new ArrayList<>();

        while (matcher.find()) {
            // 提取数字并转换为Long类型
            Long timeValue = Long.parseLong(matcher.group(2));
            timeValues.add(timeValue);

            // 如果需要，也可以打印出比较符号和时间值
            System.out.println("Found: " + matcher.group(1) + " " + timeValue);
        }

        // 打印所有找到的时间值
        if (!timeValues.isEmpty()) {
            System.out.println("Extracted time values:");
            for (Long value : timeValues) {
                System.out.println(value);
            }
        } else {
            System.out.println("No matching time values found.");
        }
    }
}
