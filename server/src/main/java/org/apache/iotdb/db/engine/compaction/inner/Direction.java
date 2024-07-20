package org.apache.iotdb.db.engine.compaction.inner;

/**
 * ClassName : Direction
 * Package :
 * Description :
 *
 * @Create :2024/7/3-15:46
 */

// 定义一个枚举类型 Direction，表示方向
public enum Direction {
    EAST, // 东
    SOUTH, // 南
    WEST, // 西
    NORTH; // 北

    // 枚举的构造方法必须是私有的
    private Direction() {
        System.out.println("调用构造方法: " + this.toString());
    }

    // 可以添加其他方法和属性
    public void show() {
        System.out.println("当前方向: " + this.toString());
    }
}

