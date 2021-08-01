package com.star.base.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: zzStar
 * @Date: 08-01-2021 11:07
 */
public class StreamingWc {

    public static void main(String[] args) throws Exception {
        // context
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // data source
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        // service
        source.flatMap((FlatMapFunction<String, String>) (value, out) -> {
                    String[] words = value.split(",");
                    for (String word : words) {
                        out.collect(word.toLowerCase().trim());
                    }
                }).filter((FilterFunction<String>) StringUtils::isNotEmpty)
                .map((MapFunction<String, Tuple2<String, Integer>>) value ->
                        new Tuple2<>(value, 1))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>)
                        value -> value.f0).sum(1).print();
        env.execute("StreamingWc");
    }
}
