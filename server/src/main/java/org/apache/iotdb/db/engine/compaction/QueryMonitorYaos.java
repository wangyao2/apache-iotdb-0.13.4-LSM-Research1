package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.*;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
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
        String statement = context.getStatement();//获取到命令行中传入的statement
        //todo 继续编写其他的数据查询格式，获取所有的访问时间间隔，统计出信息来
        if (queryPlan instanceof GroupByTimePlan){
            GroupByTimePlan groupByTimePlan = (GroupByTimePlan) queryPlan;
            long startTime = groupByTimePlan.getStartTime();
            long endTime = groupByTimePlan.getEndTime();

        } else if (queryPlan instanceof AggregationPlan) {
            AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;


        }else if (queryPlan instanceof FillQueryPlan) {
            FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
        } else if (queryPlan instanceof LastQueryPlan) {
            LastQueryPlan lastQueryPlan = (LastQueryPlan) queryPlan;
        } else if (queryPlan instanceof RawDataQueryPlan){//这个类是前面所有类的父类，所以只能放在后面去判断
            RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;//并不改变对象，只是强制转化
            //Map<String, Set<String>> deviceToMeasurements = rawDataQueryPlan.getDeviceToMeasurements();//获得这个查询计划里面涉及到的设备和序列关系
            //TODO 把查询中涉及到的所有设备和序列都统计出来，在分析看看怎么获取查询的时间戳
            IExpression expression = rawDataQueryPlan.getExpression();//获得包含时间的表达式
            if (expression instanceof GlobalTimeExpression){//处理Q2类型查询
                GlobalTimeExpression Timeexpressi = (GlobalTimeExpression)expression;//Q2类型一定是，表达式里面全都是过滤条件
                Filter Maybe_And_filter = Timeexpressi.getFilter();//分析了一下，如果使用正则表达式去解析的话，可能更耗时间
                if (Maybe_And_filter instanceof AndFilter){
                    AndFilter sure_And_filter = (AndFilter)Maybe_And_filter;
                    Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q2(Maybe_And_filter.toString());//返回的是Q2范围查询的起始时间和结束时间
                    System.out.println(leftAndRightTime);
                }
            }else if (expression instanceof SingleSeriesExpression){//处理Q3类型查询
                SingleSeriesExpression sure_SingleSeries_filter = (SingleSeriesExpression)expression;
                Pair<String, String> stringStringPair = parseAndGetTimeRange_Q3(sure_SingleSeries_filter.getFilter().toString());
                System.out.println(stringStringPair);
            }
            Set<String> allMeasurementsInDevice = rawDataQueryPlan.getAllMeasurementsInDevice("root.CPT1.g0.d2");
            List<PartialPath> deduplicatedPaths = rawDataQueryPlan.getDeduplicatedPaths();//获取被查询的路径
            List<PartialPath> paths = rawDataQueryPlan.getDeduplicatedPaths();
            for (PartialPath path : paths) {
                String measurement = path.getMeasurement();
                TSDataType dataType = path.getSeriesType();
            }
        } else {
            LOGGER.debug("Current queryPlan is {} which is not matched", queryPlan);
        }
    }
    public void analyzeTheQueryPattern(){
        System.out.println("正在分析查询样式...");
        if (QueryQRList.isEmpty()){
            System.out.println("没有需要被分析的数据");
            return;
        }
        for (QueryPlan queryPlan : QueryQRList) {


        }
    }


    private static Pair<String,String> parseAndGetTimeRange_Q2(String sinput) {
        // 正则表达式匹配时间范围
        Pattern pattern = Pattern.compile("\\d+");//匹配字符串里面所有的整数，正好可以与Q2形式匹配
        try {
            Matcher matcher = pattern.matcher(sinput);
            List<String> numbers = new ArrayList<>();
            while (matcher.find()) {
                numbers.add(matcher.group());
            }
            return new Pair<String,String>(numbers.get(0),numbers.get(1));
        }catch (Exception e){
            System.out.println("解析Q2过滤字符串时发生异常，请查看！");
            return new Pair<String,String>("0","0");
        }
    }

    public Pair<String,String> parseAndGetTimeRange_Q3(String sinput) {
        // 正则表达式匹配 "time" 后跟任意比较符号和数字
        String regex = "time\\s+([<>!=]=?)\\s+(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        try {
            Matcher matcher = pattern.matcher(sinput);
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
    public void getAnalyzedStartTime() {
        System.out.println("返回访问统计时间...");

    }

    public void getAnalyzedInterval() {
        System.out.println("返回访问统计时间...");

    }


    private class QueryPatternEstimatorYaos {

    }
}
