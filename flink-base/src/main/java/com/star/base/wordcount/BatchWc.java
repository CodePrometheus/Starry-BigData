package com.star.base.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理
 *
 * @Author: Starry
 * @Date: 11-14-2022 11:43
 */
public class BatchWc {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment.getExecutionEnvironment()
                .readTextFile("data/wc.data")
                .flatMap(new FlatMapFuncImpl())
                .groupBy(0)
                .sum(1)
                .print();
    }
}

class FlatMapFuncImpl implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String in, Collector<Tuple2<String, Integer>> out) {
        String[] split = in.split("\\s+");
        for (String word : split) {
            out.collect(Tuple2.of(word, 1));
        }
    }
}
