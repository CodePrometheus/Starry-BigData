package com.star.base.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * sink 算子将计算结果输出到不同的目标，如写入到文件、输出到指定的网络端口
 * 写入mq等等
 *
 * @Author: zzStar
 * @Date: 08-06-2021 23:42
 */
public class SinkApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9527);
        // stream.print();
        stream.print("Test");
        env.execute("SinkApp");
    }

}
