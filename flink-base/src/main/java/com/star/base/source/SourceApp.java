package com.star.base.source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * 1.基于本地集合的 source
 * 2.基于文件的 source
 * 3.基于网络套接字的 source
 * 4.自定义的source
 *
 * @Author: zzStar
 * @Date: 08-05-2021 22:07
 */
public class SourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test02(env);
        env.execute("SourceApp");
    }

    public static void test02(StreamExecutionEnvironment env) {
        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        System.out.println("source.getParallelism() = " + source.getParallelism());
        SingleOutputStreamOperator<Long> filter = source.filter((FilterFunction<Long>) value -> value >= 5);
        System.out.println("filter.getParallelism() = " + filter.getParallelism());
        filter.print();
    }

    public static void test01(StreamExecutionEnvironment env) {
        env.setParallelism(5);
        StreamExecutionEnvironment.createLocalEnvironment();
        StreamExecutionEnvironment.createLocalEnvironment(3);
        StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        System.out.println("source.getParallelism() = " + source.getParallelism());
        SingleOutputStreamOperator<String> filter = source.filter((FilterFunction<String>) value
                -> !"pk".equals(value)).setParallelism(4);
        // 机器配置决定
        System.out.println("filter.getParallelism() = " + filter.getParallelism());
        filter.print();
    }

}
