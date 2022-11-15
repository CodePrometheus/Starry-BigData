package com.star.base.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * Flink 写 redis
 *
 * @Author: Starry
 * @Date: 11-14-2022 21:46
 */
public class RedisSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // math,zhangsan,88
        // math,lisi,60
        // math,wangwu,100
        DataStreamSource<String> data = env.socketTextStream("localhost", 6666);
        // transform
        data.filter(t -> t.split(",").length > 2)
                .map((MapFunction<String, Tuple3<String, String, Long>>) value ->
                        new Tuple3<>(value.split(",")[0], value.split(",")[1],
                                Long.parseLong(value.split(",")[2])))
                .returns(new TypeHint<Tuple3<String, String, Long>>() {
                })
                .addSink(new RedisSinkImpl());
        env.execute("RedisSink");
    }
}

class RedisSinkImpl extends RichSinkFunction<Tuple3<String, String, Long>> {
    private transient Jedis jedis;

    @Override
    public void open(Configuration config) {
        String host = "127.0.0.1";
        int port = 6379, timeout = 5000;
        jedis = new Jedis(host, port, timeout);
    }

    @Override
    public void invoke(Tuple3<String, String, Long> value, Context context) {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        jedis.hset(value.f0, value.f1, String.valueOf(value.f2));
    }

    @Override
    public void close() {
        jedis.close();
    }
}
