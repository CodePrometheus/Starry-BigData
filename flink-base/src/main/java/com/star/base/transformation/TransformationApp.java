package com.star.base.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

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
        // mapStream(env);
        // filter(env);
        // flatMap(env);
        // keyBy(env);
        reduce(env);
        env.execute("TransformationApp");
    }

    /**
     * wc
     *
     * @param env
     */
    public static void reduce(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<>(value, 1);
            }
        }).keyBy(x -> x.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }).print();
    }


    public static void keyBy(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> map = source.map((MapFunction<String, Access>) value -> {
            String[] split = value.split(",");
            Long time = Long.parseLong(split[0].trim());
            String domain = split[1].trim();
            Double traffic = Double.parseDouble(split[2].trim());
            return new Access(time, domain, traffic);
        });
        // map.keyBy("domain").sum("traffic").print();
        // map.keyBy((KeySelector<Access, String>) value -> value.getDomain()).sum("traffic").print();
        map.keyBy(k -> k.getDomain()).sum("traffic").print();
    }

    public static void flatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) {
                        String[] splits = value.split(",");
                        for (String split : splits) {
                            out.collect(split);
                        }
                    }
                }).filter((FilterFunction<String>) value -> !"pk".equals(value))
                .print();
    }

    /**
     * 过滤操作，保留true
     *
     * @param env
     */
    public static void filter(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> map = source.map((MapFunction<String, Access>) value -> {
            String[] split = value.split(",");
            Long time = Long.parseLong(split[0].trim());
            String domain = split[1].trim();
            Double traffic = Double.parseDouble(split[2].trim());
            return new Access(time, domain, traffic);
        });
        SingleOutputStreamOperator<Access> filter = map.filter((FilterFunction<Access>) value -> value.getTraffic() > 4000);
        filter.print();
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
