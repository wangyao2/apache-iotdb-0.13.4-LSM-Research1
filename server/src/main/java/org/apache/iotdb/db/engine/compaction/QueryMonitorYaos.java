package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.*;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.operator.AndFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        if (queryPlan instanceof AggregationPlan) {
            AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
        }else if (queryPlan instanceof FillQueryPlan) {
            FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
        } else if (queryPlan instanceof LastQueryPlan) {
            LastQueryPlan lastQueryPlan = (LastQueryPlan) queryPlan;
        } else if (queryPlan instanceof RawDataQueryPlan){//这个类是前面所有类的父类，所以只能放在后面去判断
            RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;
            Map<String, Set<String>> deviceToMeasurements = rawDataQueryPlan.getDeviceToMeasurements();//获得这个查询计划里面涉及到的设备和序列关系
            //TODO 把查询中涉及到的所有设备和序列都统计出来，在分析看看怎么获取查询的时间戳
            IExpression expression = rawDataQueryPlan.getExpression();//获得包含时间的表达式
            if (expression instanceof GlobalTimeExpression){
                GlobalTimeExpression Timeexpressi = (GlobalTimeExpression)expression;//表达式里面全都是过滤条件
                Filter filter1 = Timeexpressi.getFilter();
                if (filter1 instanceof AndFilter){

                }
//                AndFilter filter = (AndFilter)Timeexpressi.getFilter();
//                Filter left = filter.getLeft();
//                String Timefilter = Timeexpressi.getFilter().toString();
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

    public void getAnalyzedStartTime() {
        System.out.println("返回访问统计时间...");

    }

    public void getAnalyzedInterval() {
        System.out.println("返回访问统计时间...");

    }


    private class QueryPatternEstimatorYaos {

    }
}
