package com.star.consumer;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.math.BigDecimal;

public class CustomizedRedisSink implements RedisMapper<Tuple2<Long, BigDecimal>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "CustomizedRedisSink");
    }

    @Override
    public String getKeyFromData(Tuple2<Long, BigDecimal> value) {
        return value.f0.toString();
    }

    @Override
    public String getValueFromData(Tuple2<Long, BigDecimal> value) {
        return value.f1.toString();
    }
}
