package com.star.base.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

/**
 * @Author: Starry
 * @Date: 11-14-2022 23:19
 */
public class GlobalWindowsImpl {
    public static void main(String[] args) throws Exception {
        // 触发器
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 7777)
                .map(Integer::parseInt).
                // 定义一个大小为15的滚动计数窗口 求窗口内的最小值
                        windowAll(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(15))).min(0).print();
        env.execute();
    }
}
