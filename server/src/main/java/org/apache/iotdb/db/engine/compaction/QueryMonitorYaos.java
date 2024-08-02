package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.qp.physical.crud.*;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
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

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");;
    private static final QueryMonitorYaos INSTANCE = new QueryMonitorYaos();
    private static final Logger LOGGER =
            LoggerFactory.getLogger(IoTDBConstant.COMPACTION_LOGGER_NAME);
    private static ArrayList<QueryPlan> QueryQRList = new ArrayList<>();//每一个新到达的query都添加进来作为备用
    private static ArrayList<QueryContext> ContextCTList = new ArrayList<>();//每一个新到达的query都添加进来作为备用
    private static long StartTimeFeatueOfABatchQuerys = 0;
    private static long IntervalTimeFeatueOfABatchQuerys = 0;
    private static long EndTimeFeatueOfABatchQuerys = 0;

    private static ArrayList<FeatureofOneQuery> QueryFeaturesList = new ArrayList<>();//记录每一个范围查询的查询间隔
    private static ArrayList<FeatureofOneQuery> QueryFeaturesGloablList = new ArrayList<>();//记录一批查询结果的几个特征

    //private static ArrayList<Long> QueryInterval = new ArrayList<>();//记录每一个范围查询的查询间隔
    //private static ArrayList<Long> QueryStartTime = new ArrayList<>();//记录每一个范围查询的查询开始时间

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
        ContextCTList.add(context);//暂时使用不到；现在启用，用来收集查询的开始时间
//        analyzeTheQueryFeature();//暂时先放在这里，后面要移动到合并查询之前，进行查询样式的分析
//        analyzeTheGolableFeatures();
    }

    /**
     * 遍历收集的所有查询负载，如果数量超过10条，才进行分析，否则暂时不分析
     * 用来计算收集到的所有查询负载，获得每一个查询的<起始时间，查询时间跨度，结束时间>，并把它们放到QueryFeaturesList中以备后续分析计算
     */
    public void analyzeTheQueryFeature() {
        System.out.println("正在计算序列的查询特征...");
        if (QueryQRList.size() < 10) {
            System.out.println("没有需要被分析的数据,或者搜集的查询数量过少！");
            return;
        }
        QueryFeaturesList.clear();//分析完一批之后，就清空里面的内容
        ContextCTList.clear();//分析完一批之后，就清空里面的内容

        for (QueryPlan queryPlan : QueryQRList) {//这个链表是按照查询负载的到达顺序存储的
            IExpression expression;//临时创建一个空对象指针，节省空间
            //todo 外区间范围查询暂未修复，预计不会使用
            if (queryPlan instanceof GroupByTimePlan) {
                //todo Q7 分组聚合查询，Group by time ，可以直接获得起始结束时间无需解析
                GroupByTimePlan groupByTimePlan = (GroupByTimePlan) queryPlan;
                Pair<String, String> leftAndRightTime = new Pair<>(String.valueOf(groupByTimePlan.getStartTime()), String.valueOf(groupByTimePlan.getEndTime()));
                FeatureofOneQuery featureofOneQuery = CalculatedIntervalAndStartTime(leftAndRightTime.right, leftAndRightTime.left);
                QueryFeaturesList.add(featureofOneQuery);
            } else if (queryPlan instanceof AggregationPlan) {
                //todo Q4 带时间范围的，聚合函数查询，select count(s3) from ... where time > and time <
                AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
                expression = aggregationPlan.getExpression();
                if (expression instanceof GlobalTimeExpression) {
                    Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q2(aggregationPlan.getExpression().toString());//直接解析表达式的字符串形式获得输出
                    FeatureofOneQuery featureofOneQuery = CalculatedIntervalAndStartTime(leftAndRightTime.right, leftAndRightTime.left);
                    QueryFeaturesList.add(featureofOneQuery);
                } else {//其他的情况就是expression instanceof SingleSeriesExpression
                    try {
                        //todo Q6 值过滤，和时间过滤的，聚合函数查询，select count(s3) from ... where time > ... and time < ... and root.ln.s1 >-5
                        SingleSeriesExpression sure_SingleSeries_filter = (SingleSeriesExpression) expression;
                        Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q6(sure_SingleSeries_filter.getFilter().toString());
                        FeatureofOneQuery featureofOneQuery = CalculatedIntervalAndStartTime(leftAndRightTime.right, leftAndRightTime.left);
                        QueryFeaturesList.add(featureofOneQuery);
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
                        FeatureofOneQuery featureofOneQuery = CalculatedIntervalAndStartTime(leftAndRightTime.right, leftAndRightTime.left);
                        QueryFeaturesList.add(featureofOneQuery);
                    } else {//对应了一元过滤算子
                        //todo Q1 类型查询，单个时间戳的查询，(手动添加)或者单边的时间范围查询 select s3 from ... where time > ..
                        Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q2(Maybe_And_filter.toString());
                        FeatureofOneQuery featureofOneQuery = CalculatedIntervalAndStartTime(leftAndRightTime.right, leftAndRightTime.left);
                        QueryFeaturesList.add(featureofOneQuery);
                    }
                } else if (expression instanceof SingleSeriesExpression) {//处理Q3类型查询
                    //todo Q3, Q10 类型查询，时间范围查询,外带值过滤查询 select s3 from ... where time > and time < and value >
                    SingleSeriesExpression sure_SingleSeries_filter = (SingleSeriesExpression) expression;
                    Pair<String, String> leftAndRightTime = parseAndGetTimeRange_Q3(sure_SingleSeries_filter.getFilter().toString());
                    FeatureofOneQuery featureofOneQuery = CalculatedIntervalAndStartTime(leftAndRightTime.right, leftAndRightTime.left);
                    QueryFeaturesList.add(featureofOneQuery);
                }
            } else {
                LOGGER.debug("Current queryPlan is {} which is not matched", queryPlan);
            }
        }
        for (QueryContext context : ContextCTList) {

        }
        analyzeTheQueryListToSegmentFeatures();//使用分析方法，把收到的查询负载解析成很多特征和标签样式
        analyzeTheGolableFeatures_UsingMeanShift();//使用方法分析，收集负载的特征，把负载解析成几个类型的特征，存储到QueryFeaturesGloablList内
        outputTheQueryFeatureToCsv_asTranningSample();//把收集到的负载写入到csv文件里
        QueryFeaturesList.clear();//分析完一批之后，就清空里面的内容
        QueryQRList.clear();
        if (!QueryFeaturesGloablList.isEmpty()){
            System.out.println(QueryFeaturesGloablList.get(0));
        }
        System.out.println("pasue to debug");
    }

    /**
     * 用来汇总一段时间内的，并把结果封装到FeatureofOneQuery类里面返回
     */
    private void analyzeTheQueryListToSegmentFeatures() {


    }

    /**
     * 用来计算一个查询的<起始时间，查询时间跨度，结束时间>，并把结果封装到FeatureofOneQuery类里面返回
     */
    private FeatureofOneQuery CalculatedIntervalAndStartTime(String rightTime, String leftTime) {
        long startime = Long.parseLong(leftTime);
        long endtime = Long.parseLong(rightTime);
        return new FeatureofOneQuery(startime, endtime - startime, endtime);
    }

    /**
     * 使用普通质心法去分析一批查询负载的访问特征
     */
    private void analyzeTheGolableFeatures_UsingNormalCentroid() {
        int count = QueryFeaturesList.size();
        double startTimeSum = 0.0, InetvalTimeSum = 0.0, EndTimeSum = 0.0;
        for (FeatureofOneQuery featureofOneQuery : QueryFeaturesList) {
            startTimeSum += featureofOneQuery.getStartTime();
            InetvalTimeSum += featureofOneQuery.getInterval();
            EndTimeSum += featureofOneQuery.getEndTime();
        }
        double[] centroid = {startTimeSum / count, InetvalTimeSum / count, EndTimeSum / count};//计算质心，返回一个计算过的质心对象
        QueryFeaturesGloablList.add(new FeatureofOneQuery((long) centroid[0],(long) centroid[1],(long) centroid[2]));
    }

    /**
     * 使用meanShift法去分析一批查询负载的访问特征
     */
    private void analyzeTheGolableFeatures_UsingMeanShift() {
        double bandWith = 500000.0;
        FeatureofOneQuery ARandomQuery = getRandomElement(QueryFeaturesList);//在这里进行空值的判断分析
        if (ARandomQuery != null){//如果不是空的，才进行分析
            FeatureofOneQuery OneGloableFeature = meanShift_moveToCentor(ARandomQuery, QueryFeaturesList, bandWith);
            QueryFeaturesGloablList.add(OneGloableFeature);
        }
    }

    /**
     * 使用meanShift相关的算法，传入一个随机点point，以这个点出发，寻找一个聚类中心
     */
    private FeatureofOneQuery meanShift_moveToCentor(FeatureofOneQuery point, List<FeatureofOneQuery> points, double bandwidth) {
        FeatureofOneQuery oldLastPoint = new FeatureofOneQuery(point.getStartTime(), point.getInterval(),point.getEndTime());
        boolean convergence;
        do {
            convergence = true;
            FeatureofOneQuery newPosition = meanShift_UpdataPosition(oldLastPoint, points, bandwidth);//返回一个新的位置
            System.out.println(newPosition);
            // 如果移动距离的距离较大，那么就还不收敛，然后继续搜索
            if (oldLastPoint.distanceTo(newPosition) > 100000) {
                convergence = false;
                oldLastPoint = newPosition;
            }
        } while (!convergence);
        return oldLastPoint;
    }

    /**
     * 使用meanShift相关的算法，传入一个随机点point，以这个点出发，更新下一步的移动位置
     */
    private FeatureofOneQuery meanShift_UpdataPosition(FeatureofOneQuery point, List<FeatureofOneQuery> points, double bandwidth) {
        double sumStartTime = 0;  // 初始化x坐标的总和为0
        double sumInterval = 0;  // 初始化y坐标的总和为0
        double sumEndTime = 0;  // 初始化y坐标的总和为0
        int count = 0;    // 初始化邻域内点的数量为0
        // 遍历所有点，计算邻域内点的算术平均值
        for (FeatureofOneQuery p : points) {
            double distance = point.distanceTo(p);  // 计算当前点与点集中每个点之间的欧几里得距离
            if (distance < bandwidth) {  // 如果距离小于带宽，则该点属于当前点的邻域
                sumStartTime += p.getStartTime();  // 将邻域内点的x坐标累加到sumX
                sumInterval += p.getInterval();  // 将邻域内点的y坐标累加到sumY
                sumEndTime += p.getEndTime();  // 将邻域内点的y坐标累加到sumY
                count++;      // 增加邻域内点的数量
            }
        }
        // 计算算术平均的x和y坐标
        double meanX = sumStartTime / count;  // 将x坐标的总和除以邻域内点的数量，得到算术平均的x坐标
        double meanY = sumInterval / count;  // 将y坐标的总和除以邻域内点的数量，得到算术平均的y坐标
        double meanZ = sumEndTime / count;  // 将y坐标的总和除以邻域内点的数量，得到算术平均的y坐标
        return new FeatureofOneQuery((long) meanX, (long) meanY, (long) meanZ);  // 返回计算得到的算术平均点，就算有小数也不要紧，直接截断，转换成long类型
    }

    /**
     * 使用自己编写分析方法，去分析一批查询负载的访问特征
     */
    private void analyzeTheGolableFeatures_UsingYaosClusterv1() {
        int count = QueryFeaturesList.size();
        double startTimeSum = 0.0, InetvalTimeSum = 0.0, EndTimeSum = 0.0;
        for (FeatureofOneQuery featureofOneQuery : QueryFeaturesList) {
            startTimeSum += featureofOneQuery.getStartTime();
            InetvalTimeSum += featureofOneQuery.getInterval();
            EndTimeSum += featureofOneQuery.getEndTime();
        }
        double[] centroid = {startTimeSum / count, InetvalTimeSum / count, EndTimeSum / count};//计算质心，返回一个计算过的质心对象
        QueryFeaturesGloablList.add(new FeatureofOneQuery((long) centroid[0],(long) centroid[1],(long) centroid[2]));
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

    public ArrayList<FeatureofOneQuery> getAnalyzedFeatruedList() {
        System.out.println("返回访问统计时间...");
        return QueryFeaturesGloablList;
    }

    public void clearFeatures() {
        QueryFeaturesList.clear();
        QueryFeaturesGloablList.clear();
    }

    /**
     * 从一个List中随机返回一个元素
     */
    private static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null; // 或者抛出异常，根据你的需求
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size()); // 生成一个随机索引
        return list.get(randomIndex); // 返回列表中随机索引处的元素
    }

    /**
     * 预测算法前测试，把收集到的负载全都写入到CSV文件，或者生成为ML可训练的样本
     */
    public void outputTheQueryFeatureToCsv_asTranningSample() {
        // 使用FileWriter写入文件
        FeatureofOneQuery feature = null;
        QueryContext context = null;
        try (FileWriter writer = new FileWriter("F:\\Workspcae\\IdeaWorkSpace\\IotDBMaster2\\apache-iotdb-0.13.4-LSM-Research1\\outputCsv\\a.csv")) {
            writer.write("start,interval,endtime,startQuery" + System.lineSeparator());
            for (int i = 0; i < QueryFeaturesList.size(); i++) {
                feature = QueryFeaturesList.get(i);
                context = ContextCTList.get(i);
                // 写入每个元素的toString()返回值，并在末尾添加换行符
                String oneLine = feature.startTime + "," + feature.interval + "," + feature.endTime + "," +context.getStartTime();
                writer.write(oneLine + System.lineSeparator());
            }
            System.out.println("刷写到CSV A 已完成！");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (FileWriter writer = new FileWriter("F:\\Workspcae\\IdeaWorkSpace\\IotDBMaster2\\apache-iotdb-0.13.4-LSM-Research1\\outputCsv\\b.csv")) {
            writer.write("groupNum,startTime_mean,startTime_Vari,endtime_mean,endtime_Vari,startQuery_mean,startQuery_vari" + System.lineSeparator());
            final int groupSize = 10;
            int count = 1;
            for (int i = 0; i < QueryFeaturesList.size(); i += groupSize) {
                // 获取当前组的子列表
                ArrayList<FeatureofOneQuery> group = new ArrayList<>(QueryFeaturesList.subList(i, Math.min(i + groupSize, QueryFeaturesList.size())));
                // 计算每一个小分组内的平均值和方差
                double[] stats_Startime = calculateGroupStatistics_StartTime(group);
                double startime_mean = stats_Startime[0];
                double startime_variance = stats_Startime[1];

                double[] stats_endtime = calculateGroupStatistics_endtime(group);
                double endtime_mean = stats_endtime[0];
                double endtime_variance = stats_endtime[1];

                ArrayList<QueryContext> groupCX = new ArrayList<>(ContextCTList.subList(i, Math.min(i + groupSize, ContextCTList.size())));
                double[] stats_QueryStartime = calculateGroupStatistics_StartTime_ofQuery(groupCX);
                double Qstartime_mean = stats_QueryStartime[0];
                double Qstarttime_variance = stats_QueryStartime[1];
                // 写入CSV文件
                writer.write(count++ +
                        ","+ startime_mean + "," + startime_variance +
                        "," + endtime_mean +"," +endtime_variance +
                        "," + Qstartime_mean+ "," + Qstarttime_variance
                        + System.lineSeparator());
            }
            System.out.println("刷写到CSV B 已完成！");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static double[] calculateGroupStatistics_StartTime(ArrayList<FeatureofOneQuery> group) {
        double sum = 0.0;
        double squareSum = 0.0;
        for (FeatureofOneQuery feature : group) {
            double value = feature.getStartTime();
            sum += value;
            squareSum += value * value;
        }
        double mean = sum / group.size();
        double variance = (squareSum / group.size()) - (mean * mean);
        return new double[]{mean, variance};
    }

    private static double[] calculateGroupStatistics_endtime(ArrayList<FeatureofOneQuery> group) {
        double sum = 0.0;
        double squareSum = 0.0;
        for (FeatureofOneQuery feature : group) {
            double value = feature.getEndTime();
            sum += value;
            squareSum += value * value;
        }
        double mean = sum / group.size();
        double variance = (squareSum / group.size()) - (mean * mean);
        return new double[]{mean, variance};
    }

    private static double[] calculateGroupStatistics_StartTime_ofQuery(ArrayList<QueryContext> group) {
        double sum = 0.0;
        double squareSum = 0.0;
        for (QueryContext feature : group) {
            double value = feature.getStartTime();
            sum += value;
            squareSum += value * value;
        }
        double mean = sum / group.size();
        double variance = (squareSum / group.size()) - (mean * mean);
        return new double[]{mean, variance};
    }

    private class QueryPatternEstimatorYaos {

    }

    /**
     * 用来记录每一条查询的特征，如果涉及到更多特征的话，那么再考虑追加其他特征
     */
    public class FeatureofOneQuery {
        private long startTime = 0;
        private long interval = 0;
        private long endTime = 0;

        public FeatureofOneQuery(long startTime, long interval, long endTime) {
            this.startTime = startTime;
            this.interval = interval;
            this.endTime = endTime;
        }

        @Override
        public String toString() {
            String formatstartTime = dateFormat.format(new Date(startTime));
            String formatendTime = dateFormat.format(new Date(endTime));
            return "{" +
                    "startTime=" + formatstartTime +
                    ", Interval=" + interval/1000 +
                    "s, endTime=" + formatendTime +
                    '}';
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public void setInterval(long interval) {
            this.interval = interval;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public long getInterval() {
            return interval;
        }

        public long getEndTime() {
            return endTime;
        }

        public double distanceTo(FeatureofOneQuery other) {
            double sqrt = Math.sqrt(Math.pow(this.startTime - other.startTime, 2)//这里返回的是小数，如果两个范围是1707321993000L - 0的平方，返回结果越2.4*10^24，double类型也能存住
                    + Math.pow(this.interval - other.interval, 2)
                    + Math.pow(this.endTime - other.endTime, 2));
            return sqrt;
        }
    }
}
