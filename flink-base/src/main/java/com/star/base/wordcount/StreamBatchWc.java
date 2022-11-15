package com.star.base.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;

/**
 * 流批处理
 *
 * @Author: Starry
 * @Date: 11-14-2022 11:57
 */
public class StreamBatchWc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(BATCH); // 流处理批计算
        // env.setRuntimeMode(STREAMING);
        // env.setRuntimeMode(AUTOMATIC);
        env.setParallelism(1)
                .readTextFile("data/wc.data")
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (in, out) -> {
                    for (String word : in.split("\\s+")) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                // 存在泛型擦除，返回值做约束
                // .returns(new TypeHint<Tuple2<String, Integer>>() {})
                // .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) tuple2 -> tuple2.f0).sum(1).print();
        env.execute();
    }
}
