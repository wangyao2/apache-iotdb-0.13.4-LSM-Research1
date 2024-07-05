package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.qp.physical.crud.*;
import org.apache.iotdb.db.query.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public QueryMonitorYaos() {
        System.out.println("查询负载收集器已被初始化，正在运行......");

    }

    public static QueryMonitorYaos getInstance() {
        return INSTANCE;
    }

    public void addAquery(QueryPlan queryPlan, QueryContext context) {
        //每次执行查询时，都把查询涉及到的设备和时间范围捕获过来，拿到
        if (queryPlan instanceof AggregationPlan) {
            AggregationPlan aggregationPlan = (AggregationPlan) queryPlan;
        }else if (queryPlan instanceof FillQueryPlan) {
            FillQueryPlan fillQueryPlan = (FillQueryPlan) queryPlan;
        } else if (queryPlan instanceof LastQueryPlan) {
            LastQueryPlan lastQueryPlan = (LastQueryPlan) queryPlan;
        } else if (queryPlan instanceof RawDataQueryPlan){//这个类是前面所有类的父类，所以只能放在后面去判断
            RawDataQueryPlan rawDataQueryPlan = (RawDataQueryPlan) queryPlan;

        } else {
            LOGGER.debug("Current queryPlan is {} which is not matched", queryPlan);
        }

    }
    public void analyzeTheQueryPattern(){
        System.out.println("正在分析...");
    }

    private class QueryPatternEstimatorYaos {

    }
}
