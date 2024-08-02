package org.apache.iotdb.db.engine.compaction;
import weka.classifiers.Classifier;
import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.classifiers.Evaluation;
/**
 * ClassName:MLQueryAnalyzerYaos
 * Description: 用来分析收到的查询负载，周期性的将他们训练成模型，预计
 *
 * @Create:2024/7/29 -17:25
 */
public class MLQueryAnalyzerYaos {

    public static void main(String[] args) throws Exception {
        // 加载数据
        DataSource source = new DataSource("F:\\ProgramFiles\\Weka-3-8-6\\data\\diabetes.arff");
        Instances data = source.getDataSet();

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
