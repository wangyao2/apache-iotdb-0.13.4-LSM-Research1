package org.apache.iotdb.db.engine.compaction;
import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomForest;
import weka.core.*;
import weka.core.converters.ConverterUtils.DataSource;
import weka.classifiers.Evaluation;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 用来分析收到的查询负载，将他们训练成模型，并给出接下来的范围查询，可能会涉及到的数据查询范围
 * 使用单例模式，确保仅存在一个单一的模型
 *
 * @Create:2024/7/29 -17:25
 */
public class MLQueryAnalyzerYaos {

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");;
    private static final MLQueryAnalyzerYaos MLINSTANCE = new MLQueryAnalyzerYaos();

    //记录一批（一组）查询结果的特征，用于构造训练集的准备
    private static ArrayList<QueryMonitorYaos.FeatureofGroupQuery> QuerySegmentFeatures = new ArrayList<>();
    private String Arrtibutes = "groupNum,start_mean,start_varian,end_mean,end_varian,QBegintime_mean,QBegintime_Varian,targetStartTime";

    public MLQueryAnalyzerYaos() {
        System.out.println("ML分析器已被初始化，正在运行......");
    }

    public static MLQueryAnalyzerYaos getInstance() {
        return MLINSTANCE;
    }

    /**
     * 把查询负载监视器收集到查询负载存储起来
     */
    public void setQuery(ArrayList<QueryMonitorYaos.FeatureofGroupQuery> tranningSet){
        QuerySegmentFeatures.clear();//先清空，再复制进来

        QuerySegmentFeatures.addAll(tranningSet);//浅复制，对现在的元素操作也会影响到之前的操作
    }

    /**
     * 对外暴露，用于构建数据集，训练模型，并且给出预测结果
     */
    public long[] TranningAndPredict() throws Exception {
        long[] startTime_And_EndTime = new long[2];

        //1 解析数据成为可训练样本
        // 声明样本集的特征有哪些，特征属性，已经声明在前面了，一个string 类型的字符串，方便后续调用使用
        ArrayList<Attribute> arrt = new ArrayList<>();
        String[] arrtibutts = Arrtibutes.split(",");

        int[] TranningDataindices = {0, 1, 3, 5};//从原始样本集中，提取某几列作为样本集
        for (int index : TranningDataindices) {
            arrt.add(new Attribute(arrtibutts[index]));
        }
        arrt.add(new Attribute(arrtibutts[arrtibutts.length - 1]));//这里单独列出来，把最后一个元素存入
        int ArrtSize = arrt.size();//样本集中属性的长度，包含末尾的标签

        double[] sample; //7维
        double[] downSamplingSample = new double[TranningDataindices.length];
        Instances TranningData = new Instances("dataset", arrt, 0);//创建用于训练的样本数据
        Instances TranningData_EndTime = new Instances("dataset", arrt, 0);//创建用于训练的样本数据

        int SegmentNums = QuerySegmentFeatures.size();//判断收集到了多少个 可以训练的样本组，最大组数，用来协助划分
        //for循环遍历每一个收集到的 样本段，将其封装成一个可用训练集
        for (int g = 0; g < SegmentNums; g++){ //不能用增强for循环，这样就没法访问到相邻的其他元素了
            QueryMonitorYaos.FeatureofGroupQuery QuerySegmentWithFeature = QuerySegmentFeatures.get(g);
            sample = QuerySegmentWithFeature.toDoubleValueArray_TargetStartTime();//把内部所有的属性的数值都转化成一个double[]数组，方便构建可训练的实例对象
            int count = 0;
            for (int index : TranningDataindices) {
                downSamplingSample[count++] = sample[index];//只保留关注的那部分属性列，没有填充 target列
            }
            //下面的if和else 用于样本的填充，同时和判断
            if (g == SegmentNums -1|| (int) sample[0] == SegmentNums){//sample的第一个元素是组号，如果是最后一个组，则无需，也无法获得下一个时间段的 查询值 ，将其作为预测对象
                //
                double[] newArray_filledWithTarget = new double[ArrtSize];//这里包含了target标签的长度
                double[] newArray_filledWithTarget_EndTime = new double[ArrtSize];//这里包含了target标签的长度，预测访问末尾时间的样本集

                System.arraycopy(downSamplingSample, 0, newArray_filledWithTarget, 0, downSamplingSample.length);//前n-1个元素作为X，样本集，最后一个元素，使用标签y填充，用下一个时刻的值填充
                System.arraycopy(downSamplingSample, 0, newArray_filledWithTarget_EndTime, 0, downSamplingSample.length);//前n-1个元素作为X，样本集，最后一个元素，使用标签y填充，用下一个时刻的值填充

                newArray_filledWithTarget[downSamplingSample.length] = 0;//如果是最后一个元素，那么就没有训练的标签，应该作为 被预测的对象使用
                newArray_filledWithTarget_EndTime[downSamplingSample.length] = 0;//如果是最后一个元素，那么就没有训练的标签，应该作为 被预测的对象使用

                TranningData.add(new DenseInstance(1.0,newArray_filledWithTarget));//用数组封装构建成样本
                TranningData_EndTime.add(new DenseInstance(1.0,newArray_filledWithTarget_EndTime));//用数组封装构建成样本
            } else{//其他组（不是最后一个组），就把下一个时刻的 起始时间作为预测的 标签y，放入到sample数据的最后一个元素
                double[] newArray_filledWithTarget = new double[ArrtSize];//这里填入target标签的长度，整个样本的长度已经
                double[] newArray_filledWithTarget_EndTime = new double[ArrtSize];//这里包含了target标签的长度

                System.arraycopy(downSamplingSample, 0, newArray_filledWithTarget, 0, downSamplingSample.length);//前n-1个元素作为X，样本集，最后一个元素，使用标签y填充，用下一个时刻的值填充
                System.arraycopy(downSamplingSample, 0, newArray_filledWithTarget_EndTime, 0, downSamplingSample.length);//前n-1个元素作为X，样本集，最后一个元素，使用标签y填充，用下一个时刻的值填充

                //newArray数组的最后一个元素,用下一个分段的值填充,例如sample[1]，把下一个时间段的起始时间作为预测目标值
                newArray_filledWithTarget[downSamplingSample.length] = QuerySegmentFeatures.get(g+1).toDoubleValueArray_TargetStartTime()[1];//一个段的开始时间下标1
                newArray_filledWithTarget_EndTime[downSamplingSample.length] = QuerySegmentFeatures.get(g+1).toDoubleValueArray_TargetStartTime()[3];//一个段的结束时间下标3

                TranningData.add(new DenseInstance(1.0,newArray_filledWithTarget));//用数组封装构建成样本
                TranningData_EndTime.add(new DenseInstance(1.0,newArray_filledWithTarget_EndTime));//用数组封装构建成样本
            }
        }
        //2 构建模型
        // 设置类别属性，默认把属性的最后一列，当作预测的标签列
        if (TranningData.classIndex() == -1) TranningData.setClassIndex(TranningData.numAttributes() - 1);
        if (TranningData_EndTime.classIndex() == -1) TranningData_EndTime.setClassIndex(TranningData_EndTime.numAttributes() - 1);

        // todo 补充划分训练集和测试集的内容
        // 划分数据集

        int testSize = 1; // 测试集大小
        int trainSize = TranningData.numInstances() - testSize; // 训练集大小

        Instances train_SpiltData_StartTime = new Instances(TranningData, 0, trainSize);//预测起始时间
        Instances train_SpiltData_EndTime = new Instances(TranningData_EndTime, 0, trainSize);//预测起始时间

        //Instances test = new Instances(TranningData, trainSize, testSize);

        train_SpiltData_StartTime.randomize(new java.util.Random(0)); // 训练集打散
        train_SpiltData_EndTime.randomize(new java.util.Random(0)); // 训练集打散

        RandomForest model_StartTime = new RandomForest();
        model_StartTime.buildClassifier(train_SpiltData_StartTime);

        RandomForest model_EndTime = new RandomForest();
        model_EndTime.buildClassifier(train_SpiltData_EndTime);


        //3 给出预测结果
        Instance StartTime_lastInstance = TranningData.lastInstance();
        System.out.println("被预测样本：" + StartTime_lastInstance);
        double Predicted_startTime = model_StartTime.classifyInstance(StartTime_lastInstance);
        System.out.println("预测的时间戳：" + Predicted_startTime);

        Instance TestInstance1 = TranningData.instance(TranningData.size() - 2);//选择前面可能的样本进行预测
        System.out.println("被预测样本" + TestInstance1);
        System.out.println("预测的时间戳：" + model_StartTime.classifyInstance(TestInstance1));

        Instance TestInstance2 = TranningData.instance(TranningData.size() - 4);//选择前面可能的样本进行预测
        System.out.println("被预测样本" + TestInstance2);
        System.out.println("预测的时间戳：" + model_StartTime.classifyInstance(TestInstance2));

        startTime_And_EndTime[0] = (long) Predicted_startTime;
        //++++++++++++++++++++前面预测++++++下一个查询涉及的起始时间++++++++++++++++++
        //++++++++++++++++++++下面预测++++++下一个查询涉及的起始时间++++++++++++++++++

        Instance EndTime_lastInstance = TranningData_EndTime.lastInstance();
        System.out.println("被预测样本，endtime：" + EndTime_lastInstance);
        double Predicted_endTime = model_EndTime.classifyInstance(EndTime_lastInstance);
        System.out.println("预测的时间戳，endtime：" + Predicted_endTime);

        startTime_And_EndTime[1] = (long) Predicted_endTime;
        QuerySegmentFeatures.clear();//这个列表的清空，也会导致QueryMonitor中的元素清空
        return startTime_And_EndTime;
    }

    /**
     * 对外暴露，训练模型，通过聚类等方法获取最近一批查询密度最高的地方，这一部分由负载收集器去处理
     */
    public long[] ClusteringTheCurrentQueryRrange() {
        long[] startTime_And_EndTime = new long[2];
        startTime_And_EndTime[0] = 0;
        startTime_And_EndTime[1] = 0;
        return startTime_And_EndTime;
    }

    /**
     * 把查询负载监视器收集到的封装成可以训练的数据集
     */
    private void constructTranningDataSet(){

    }

    private void predictTheFuture(){

    }

    public static void main(String[] args) throws Exception {
        // 加载数据
        DataSource source = new DataSource("F:\\ProgramFiles\\Weka-3-8-6\\data\\diabetes.arff");
        Instances data = source.getDataSet();
        Instance instance5 = data.get(1);
        System.out.println(instance5);
        // 创建属性列表
        FastVector attributes = new FastVector(3); // 假设有三个特征
        attributes.addElement(new Attribute("feature1"));
        attributes.addElement(new Attribute("feature2"));
        attributes.addElement(new Attribute("feature3"));

        ArrayList<Attribute> arrt = new ArrayList<>();
        arrt.add(new Attribute("feature1"));
        arrt.add(new Attribute("feature2"));
        arrt.add(new Attribute("feature3"));
        Instances data2 = new Instances("dataset", arrt, 0);

        // 创建实例并添加到Instances对象
        Instance instance1 = new SparseInstance(3);
        instance1.setValue(data2.attribute("feature1"), 1.0);
        instance1.setValue(data2.attribute("feature2"), 2.0);
        instance1.setValue(data2.attribute("feature3"), 3.0);
        data2.add(instance1);

        Instance instance2 = new SparseInstance(3);
        instance2.setValue(data2.attribute("feature1"), 2.0);
        instance2.setValue(data2.attribute("feature2"), 3.0);
        instance2.setValue(data2.attribute("feature3"), 4.0);
        data2.add(instance2);

        double[] myArray = {1.1, 2.2, 3.3};
        Instance instance3 = new DenseInstance(1.0,myArray);
        data2.add(instance3);

        Instance instance4 = new DenseInstance(1.0,myArray);
        data2.add(instance4);

        Instance instance55 = new DenseInstance(1.0,myArray);
        data2.add(instance55);


        Instance instance6 = new DenseInstance(1.0,myArray);
        data2.add(instance6);
        // 设置类别属性
        if (data.classIndex() == -1)
            data.setClassIndex(data.numAttributes() - 1);

        // 按照70%和30%的比例划分训练集和测试集
        data.randomize(new java.util.Random(0)); // 设置随机种子
        int trainSize = (int) Math.round(data.numInstances() * 0.7);
        int testSize = data.numInstances() - trainSize;
        Instances trainData = new Instances(data, 0, trainSize);
        Instances testData = new Instances(data, trainSize, testSize);

        // 创建随机森林模型
        RandomForest model = new RandomForest();

        // 训练模型
        model.buildClassifier(trainData);

        Instance instance = testData.instance(1);
        System.out.println(instance);
        double v = model.classifyInstance(instance);
        System.out.println(v);
        // 使用测试集评估模型
        Evaluation eval = new Evaluation(trainData);
        double[] doubles = eval.evaluateModel(model, testData);

        // 获取平均精确率
        double precision = eval.weightedPrecision();
        System.out.println("Weighted Precision: " + precision);

        // 如果需要获取特定类别的精确率，确保类别索引是正确的
        double classPrecision = eval.precision(0);
        System.out.println("classPrecision" + classPrecision);

        classPrecision = eval.precision(1);
        System.out.println("classPrecision" + classPrecision);
        // 输出混淆矩阵
        System.out.println(eval.toMatrixString());
        // 输出测试集中的每一个预测结果
//        for (int i = 0; i < testData.numInstances(); i++) {
//            double pred = model.classifyInstance(testData.instance(i));
//            System.out.println("Instance " + (i + 1) + ": Actual=" + testData.classAttribute().value((int) testData.instance(i).classValue()) +
//                    ", Predicted=" + testData.classAttribute().value((int) pred));
//        }
    }

}
