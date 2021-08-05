package com.star.base.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: zzStar
 * @Date: 08-05-2021 22:57
 */
public class TransformationApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // map(env);
        mapStream(env);
        env.execute("TransformationApp");
    }


    public static void map(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> map = source.map((MapFunction<String, Access>) value -> {
            String[] split = value.split(",");
            Long time = Long.parseLong(split[0].trim());
            String domain = split[1].trim();
            Double traffic = Double.parseDouble(split[2].trim());
            return new Access(time, domain, traffic);
        });
        map.print();
    }

    public static void mapStream(StreamExecutionEnvironment env) {
        List<Integer> list = Arrays.asList(1, 2, 3);
        DataStreamSource<Integer> source = env.fromCollection(list);
        source.map((MapFunction<Integer, Integer>) value -> value * 2).print();
    }

}
