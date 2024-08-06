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
        for (String arrtibutt : arrtibutts) {
            arrt.add(new Attribute(arrtibutt));
        }
        int ArrtSize = arrt.size();//样本集中属性的长度，包含末尾的标签
        double[] sample;
        Instances TranningData = new Instances("dataset", arrt, 0);
        int SegmentNums = QuerySegmentFeatures.size();//判断收集到了多少个 可以训练的样本组，最大组数，用来协助划分
        for (QueryMonitorYaos.FeatureofGroupQuery querySegmentFeature : QuerySegmentFeatures) {
            //QuerySegmentFeatures是从外部的QueryMonitor里面获取到的，一组一组的查询负载
            sample = querySegmentFeature.toDoubleValueArray_TargetStartTime();//把内部所有的属性的数值都转化成一个double[]数组，方便构建可训练的实例对象
            if ((int) sample[0] == SegmentNums){//如果是最后一个组，则无需，也无法获得下一个时间段的 查询值

            } else{//其他组，就把下一个时刻的 起始时间作为预测的 标签y，放入到sample数据的最后一个元素
                double[] newArray = new double[ArrtSize];
                System.arraycopy(sample, 0, newArray, 0, sample.length);//前n-1个元素作为X，样本集，最后一个元素，使用标签y填充，用下一个时刻的值填充
                newArray[sample.length] = 0;//最后一个元素用下一个时刻的值填充,例如sample[1]，就是把起一个时间段的起始时间作为预测目标值
            }
            if (sample.length == ArrtSize){//在封装成可训练样本之前，先判断数据是不是能与 属性的数量匹配起来
                TranningData.add(new DenseInstance(1.0,sample));
            }else {
                System.out.println("不匹配的属性长度，来源于MLQueryAnalyzerYaos 60行代码.....");
            }
        }
        //2 构建模型
        // 设置类别属性，默认把属性的最后一列，当作预测的标签列
        //if (TranningData.classIndex() == -1) TranningData.setClassIndex(TranningData.numAttributes() - 1);

        RandomForest model = new RandomForest();

        //model.buildClassifier(TranningData);

        //3 给出预测结果，封装到list内
        //Instance TestInstance = TranningData.instance(TranningData.size() - 1);
        //double startTime = model.classifyInstance(TestInstance);
        startTime_And_EndTime[0] = (long) 1;
        //++++++++++++++++++++前面预测++++++下一个查询涉及的起始时间++++++++++++++++++
        //++++++++++++++++++++下面预测++++++下一个查询涉及的起始时间++++++++++++++++++


        startTime_And_EndTime[1] = (long) 2;
        QuerySegmentFeatures.clear();//这个列表的清空，也会导致QueryMonitor中的元素清空
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
