package com.star.base.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
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
