package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.*;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ClassName : QueryMonitorYaos
 * Package :
 * Description :用来监视iotdb接收到的查询负载状态，并进行分析
 * 在VSG每次执行合并方法executeCompaction之前，进行当前负载的分析
 *
 * @Create :2024/7/5-16:46
 */
public class QueryMonitorYaos {

    private static final QueryMonitorYaos INSTANCE = new QueryMonitorYaos();
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
    private static ArrayList<QueryPlan> QueryQRList = new ArrayList<>();//每一个新到达的query都添加进来作为备用
    private static ArrayList<QueryContext> ContextCTList = new ArrayList<>();//每一个新到达的query都添加进来作为备用


    public QueryMonitorYaos() {
        System.out.println("查询负载收集器已被初始化，正在运行......");
    }

    public static QueryMonitorYaos getInstance() {
        return INSTANCE;
    }

    public void addAquery(QueryPlan queryPlan, QueryContext context) {
        //每次执行查询时，都把查询涉及到的设备和时间范围捕获过来，拿到
        LOGGER.debug("接收到查询请求！ - {}", queryPlan);
        QueryQRList.add(queryPlan);
        ContextCTList.add(context);
        IExpression expression;//临时创建一个空对象指针，节省空间
        //todo 外区间范围查询暂未修复，预计不会使用
        if (queryPlan instanceof GroupByTimePlan) {
            //todo Q7 分组聚合查询，Group by time ，可以直接获得起始结束时间无需解析
            GroupByTimePlan groupByTimePlan = (GroupByTimePlan) queryPlan;
            Pair<String, String> leftAndRightTime = new Pair<>(String.valueOf(groupByTimePlan.getStartTime()), String.valueOf(groupByTimePlan.getEndTime()));
        } else if (queryPlan instanceof AggregationPlan) {
            //todo Q4 带时间范围的，聚合函数查询，select count(s3) from ... where time > and time <
            AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
            expression = aggregationPlan.getExpression();
            if (expression instanceof GlobalTimeExpression) {
                Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q2(aggregationPlan.getExpression().toString());//直接解析表达式的字符串形式获得输出
                System.out.println(leftAndRightTime);
            } else {//其他的情况就是expression instanceof SingleSeriesExpression
                try {
                    //todo Q6 值过滤，和时间过滤的，聚合函数查询，select count(s3) from ... where time > ... and time < ... and root.ln.s1 >-5
                    SingleSeriesExpression sure_SingleSeries_filter = (SingleSeriesExpression) expression;
                    Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q6(sure_SingleSeries_filter.getFilter().toString());
                    System.out.println(leftAndRightTime);
                } catch (Exception e) {//如果解析Q6发生异常，那么就认为是Q5
                    //todo Q5 值过滤的聚合函数查询，select count(s3) from ... where root.ln.s1 >-5
                    System.out.println(e.getMessage());
                    System.out.println("捕获Q5类型的查询，忽略之");
                }
            }
        } else if (queryPlan instanceof FillQueryPlan) {
            FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
        } else if (queryPlan instanceof LastQueryPlan) {
            LastQueryPlan lastQueryPlan = (LastQueryPlan) queryPlan;
        } else if (queryPlan instanceof RawDataQueryPlan) {//这个类是前面所有类的父类，所以只能放在后面去判断
            RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;//并不改变对象，只是强制转化
            //Map<String, Set<String>> deviceToMeasurements = rawDataQueryPlan.getDeviceToMeasurements();//获得这个查询计划里面涉及到的设备和序列关系
            expression = rawDataQueryPlan.getExpression();//获得包含时间的表达式
            //todo Q2, Q9 类型查询，纯时间范围查询，可以拦截到单边范围的查询 select s3 from ... where time > and time <
            if (expression instanceof GlobalTimeExpression) {//处理Q2类型查询
                GlobalTimeExpression Timeexpressi = (GlobalTimeExpression) expression;//Q2类型一定是，表达式里面全都是过滤条件
                Filter Maybe_And_filter = Timeexpressi.getFilter();//分析了一下，如果使用正则表达式去解析的话，可能更耗时间
                if (Maybe_And_filter instanceof AndFilter) {
                    Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q2(Maybe_And_filter.toString());//返回的是Q2范围查询的起始时间和结束时间
                    System.out.println(leftAndRightTime);
                } else {//对应了一元过滤算子
                    //todo Q1 类型查询，单个时间戳的查询，(手动添加)或者单边的时间范围查询 select s3 from ... where time > ..
                    Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q2(Maybe_And_filter.toString());
                    System.out.println(leftAndRightTime);
                }
            } else if (expression instanceof SingleSeriesExpression) {//处理Q3类型查询
                //todo Q3, Q10 类型查询，时间范围查询,外带值过滤查询 select s3 from ... where time > and time < and value >
                SingleSeriesExpression sure_SingleSeries_filter = (SingleSeriesExpression) expression;
                Pair<String, String> stringStringPair = parseAndGetTimeRange_Q3(sure_SingleSeries_filter.getFilter().toString());
                System.out.println(stringStringPair);
            }
        } else {
            LOGGER.debug("Current queryPlan is {} which is not matched", queryPlan);
        }
    }

    public void analyzeTheQueryPattern() {
        System.out.println("正在分析查询样式...");
        if (QueryQRList.isEmpty()) {
            System.out.println("没有需要被分析的数据");
            return;
        }
        for (QueryPlan queryPlan : QueryQRList) {


        }
    }


    private static Pair<String, String> parseAndGetTimeRange_Q2(String sinput) {
        // 正则表达式匹配时间范围 Q2和Q4都用这个解析
        Pattern pattern = Pattern.compile("\\d+");//匹配字符串里面所有的整数，正好可以与Q2形式匹配
        try {
            Matcher matcher = pattern.matcher(sinput);
            List<String> numbers = new ArrayList<>();
            while (matcher.find()) {
                numbers.add(matcher.group());
            }
            if (numbers.size() == 1) {
                pattern = Pattern.compile("([<>!=]=?)");
                matcher = pattern.matcher(sinput);
                String logicalSign = "0";
                if (matcher.find()) {
                    // 返回捕获组中的逻辑运算符
                    logicalSign = matcher.group(1);//分析这个数是
                }
                if (logicalSign.startsWith(">")) {//时间大于 xxxx
                    return new Pair<String, String>(numbers.get(0), String.valueOf(System.currentTimeMillis()));//对应π0查询
                } else if (logicalSign.startsWith("<")) {//时间小于 xxx
                    return new Pair<String, String>("0", numbers.get(0));
                } else {
                    return new Pair<String, String>(numbers.get(0), "==");
                }
            }
            return new Pair<String, String>(numbers.get(0), numbers.get(1));
        } catch (Exception e) {
            System.out.println("解析Q2过滤字符串时发生异常，请查看！");
            return new Pair<String, String>("0", "0");
        }
    }

    public Pair<String, String> parseAndGetTimeRange_Q3(String sinput) {
        // 正则表达式匹配 "time" 后跟任意比较符号和数字
        //Q3 类型查询，时间范围查询,外带值过滤查询 select s3 from ... where time > and time < and value >
        String regex = "time\\s+([<>!=]=?)\\s+(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        try {
            Matcher matcher = pattern.matcher(sinput);
            List<String> timeValues = new ArrayList<>();
            List<String> logicalSignList = new ArrayList<>();
            while (matcher.find()) {
                logicalSignList.add(matcher.group(1));
                timeValues.add(matcher.group(2));
            }
            if (timeValues.isEmpty()) {//如果结果不是空的就直接返回
                System.out.println("解析Q3过滤字符串时发生异常，请查看！");
                return new Pair<>("0", "0");
            } else if (timeValues.size() == 2) {
                return new Pair<>(timeValues.get(0), timeValues.get(1));
            } else {//单时间范围判断
                String logicalSign = logicalSignList.get(0);
                if (logicalSign.startsWith(">")) {//时间大于 xxxx
                    return new Pair<String, String>(timeValues.get(0), String.valueOf(System.currentTimeMillis()));//对应π0查询
                } else if (logicalSign.startsWith("<")) {//时间小于 xxx
                    return new Pair<String, String>("0", timeValues.get(0));
                } else {
                    return new Pair<String, String>(timeValues.get(0), "==");
                }
            }
        } catch (Exception e) {
            System.out.println("解析Q3过滤字符串时发生异常，请查看！");
            return new Pair<String, String>("0", "0");
        }
    }

    public Pair<String, String> parseAndGetTimeRange_Q6(String sinput) throws Exception {
        // 正则表达式匹配 "time" 后跟任意比较符号和数字
        String regex = "time\\s+([<>!=]=?)\\s+(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        try {
            Matcher matcher = pattern.matcher(sinput);
            List<String> timeValues = new ArrayList<>();
            List<String> logicalSignList = new ArrayList<>();
            while (matcher.find()) {
                logicalSignList.add(matcher.group(1));
                timeValues.add(matcher.group(2));
            }
            // 打印所有找到的时间值
            if (timeValues.isEmpty()) {
                System.out.println("解析Q6为空值，可能解析到了Q5，请查看！");
                return new Pair<>("0", "0");
            } else if (timeValues.size() == 2) {
                return new Pair<>(timeValues.get(0), timeValues.get(1));
            } else {//只有单边时间
                String logicalSign = logicalSignList.get(0);
                if (logicalSign.startsWith(">")) {//时间大于 xxxx
                    return new Pair<String, String>(timeValues.get(0), String.valueOf(System.currentTimeMillis()));//对应π0查询
                } else if (logicalSign.startsWith("<")) {//时间小于 xxx
                    return new Pair<String, String>("0", timeValues.get(0));
                } else {
                    return new Pair<String, String>(timeValues.get(0), "==");
                }
            }
        } catch (Exception e) {
            System.out.println("解析Q5过滤字符串时发生异常，请查看！");
            throw new Exception("解析Q5过滤字符串时发生异常，请查看！");
        }
    }

    public void getAnalyzedStartTime() {
        System.out.println("返回访问统计时间...");

    }

    public void getAnalyzedInterval() {
        System.out.println("返回访问统计时间...");

    }


    private class QueryPatternEstimatorYaos {

    }
}
