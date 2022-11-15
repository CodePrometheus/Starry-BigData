package com.star.base.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理
 *
 * @Author: zzStar
 * @Date: 08-01-2021 11:07
 */
public class StreamWc {

    public static final String SPLIT = "\\s+";

    public static void main(String[] args) throws Exception {
        // context
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // data source
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        // service
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String in, Collector out) {
                for (String word : in.split(SPLIT)) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple2 -> tuple2.f0).sum("f1").print();
        env.execute("StreamingWc");
    }
}
