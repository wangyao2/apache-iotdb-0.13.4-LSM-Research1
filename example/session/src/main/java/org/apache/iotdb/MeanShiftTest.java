package org.apache.iotdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * ClassName : MeanShiftTest
 * Package :
 * Description :
 *
 * @Create :2024/7/20-14:49
 */
public class MeanShiftTest {

    // 定义一个点类，包含x和y坐标
    static class Point {
        double x, y;

        Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        // 计算两个点之间的欧几里得距离
        double distanceTo(Point other) {
            return Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
        }

        @Override
        public String toString() {
            return "Point{" +
                    "x=" + x +
                    ", y=" + y +
                    '}';
        }
    }

    // Mean Shift算法的主要实现
    public static List<Point> meanShift(List<Point> points, double bandwidth) {
        List<Point> shiftedPoints = new ArrayList<>(points.size());
        List<Point> convergencePoints = new ArrayList<>(points.size());
        Point randomElement = getRandomElement(points);

//        for (Point point : points) {
//
        Point shiftedPoint = meanShift(randomElement, points, bandwidth);
        shiftedPoints.add(shiftedPoint);
        convergencePoints.add(shiftedPoint);
        //}

        return convergencePoints;
    }

    // 对单个点执行Mean Shift迭代
    private static Point meanShift(Point point, List<Point> points, double bandwidth) {
        Point shiftedPoint = new Point(point.x, point.y);
        boolean convergence;

        do {
            convergence = true;
            Point mean = calculateMean(shiftedPoint, points, bandwidth);
            System.out.println(mean);
            // 如果移动距离大于阈值，则继续迭代
            if (shiftedPoint.distanceTo(mean) > 1e-5) {
                convergence = false;
                shiftedPoint = mean;
            }
        } while (!convergence);

        return shiftedPoint;
    }

    // 计算均值点
    private static Point calculateMeanWithGoh(Point point, List<Point> points, double bandwidth) {
        double meanX = 0;
        double meanY = 0;
        double weightSum = 0;

        for (Point p : points) {
            double distance = point.distanceTo(p);//计算两个点之间的欧式距离来表示
            if (distance < bandwidth) {
                double weight = Math.exp(-Math.pow(distance, 2) / (2 * Math.pow(bandwidth, 2)));
                meanX += p.x * weight;
                meanY += p.y * weight;
                weightSum += weight;
            }
        }

        meanX /= weightSum;
        meanY /= weightSum;

        return new Point(meanX, meanY);
    }

    private static Point calculateMean(Point point, List<Point> points, double bandwidth) {
        double sumX = 0;  // 初始化x坐标的总和为0
        double sumY = 0;  // 初始化y坐标的总和为0
        int count = 0;    // 初始化邻域内点的数量为0

        // 遍历所有点，计算邻域内点的算术平均值
        for (Point p : points) {
            double distance = point.distanceTo(p);  // 计算当前点与点集中每个点之间的欧几里得距离
            if (distance < bandwidth) {  // 如果距离小于带宽，则该点属于当前点的邻域
                sumX += p.x;  // 将邻域内点的x坐标累加到sumX
                sumY += p.y;  // 将邻域内点的y坐标累加到sumY
                count++;      // 增加邻域内点的数量
            }
        }

        // 计算算术平均的x和y坐标
        double meanX = sumX / count;  // 将x坐标的总和除以邻域内点的数量，得到算术平均的x坐标
        double meanY = sumY / count;  // 将y坐标的总和除以邻域内点的数量，得到算术平均的y坐标

        return new Point(meanX, meanY);  // 返回计算得到的算术平均点
    }

    public static void main(String[] args) {
        // 示例：创建点集
        List<Point> points = new ArrayList<>();
        points.add(new Point(1, 1));
        points.add(new Point(2, 2));
        points.add(new Point(3, 8));
        points.add(new Point(5, 4));
        points.add(new Point(7, 7));

        // 设置带宽
        double bandwidth = 20.0;

        // 执行Mean Shift算法
        List<Point> shiftedPoints = meanShift(points, bandwidth);

        // 打印结果
        for (Point p : shiftedPoints) {
            System.out.println("(" + p.x + ", " + p.y + ")");
        }
    }

    public static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null; // 或者抛出异常，根据你的需求
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size()); // 生成一个随机索引
        return list.get(randomIndex); // 返回列表中随机索引处的元素
    }
}
