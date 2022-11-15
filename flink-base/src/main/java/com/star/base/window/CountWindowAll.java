package com.star.base.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Starry
 * @Date: 11-14-2022 23:12
 */
public class CountWindowAll {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);
        // 没有keyBy，调用countWindowAll，按照条数划分窗口，当窗口中的数据达到一定的条数再输出计算结果
        nums.countWindowAll(5)
                .sum(0).print(); // 划分窗口后，需要调用相应的方法，window operator
        env.execute();

        //1
        //2
        //3
        //4
        //5

        //4>15

        //1
        //1
        //1
        //1
        //1

        //2>5

        // 五条输出一次，没到五条永远不输出，输出结果可以做一些聚合操作等
        // 对于窗口内部进行处理的操作，是来一条计算一条，到五条发出，而不是攒五条发出前再计算五条的和，即增量聚合
    }
}
