package com.star.advance.mapfunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: zzStar
 * @Date: 08-07-2021 22:12
 */
public class PkMapFunction extends RichMapFunction<String, Access> {

    /**
     * 初始化操作
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("=========open==========");
    }

    /**
     * 清理操作
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
    }


    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }


    @Override
    public Access map(String value) throws Exception {
        System.out.println("=========map==========");
        String[] split = value.split(",");
        Long time = Long.parseLong(split[0].trim());
        String domain = split[1].trim();
        Double traffic = Double.parseDouble(split[2].trim());
        return new Access(time, domain, traffic);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(3);
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> mapStream = source.map(new PkMapFunction());
        mapStream.print();
        env.execute("PkMapFunction");
    }

}
