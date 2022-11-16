package com.star.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class FlinkToRedis {
    private final static Logger logger = LoggerFactory.getLogger(FlinkToRedis.class);

    /**
     * 序列化
     */
    public static class ProductBeanJSONDeSerializer implements KafkaDeserializationSchema<ProductBean> {
        private final String encoding = "UTF8";
        private final boolean includeTopic;
        private final boolean includeTimestamp;

        public ProductBeanJSONDeSerializer(boolean includeTopic, boolean includeTimestamp) {
            this.includeTopic = includeTopic;
            this.includeTimestamp = includeTimestamp;
        }

        @Override
        public TypeInformation<ProductBean> getProducedType() {
            return TypeInformation.of(ProductBean.class);
        }

        @Override
        public boolean isEndOfStream(ProductBean nextElement) {
            return false;
        }

        @Override
        public ProductBean deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
            if (consumerRecord != null) {
                try {
                    String value = new String(consumerRecord.value(), encoding);
                    return JSON.parseObject(value, ProductBean.class);
                } catch (Exception e) {
                    logger.error(">>>>>>deserialize failed : " + e.getMessage(), e);
                }
            }
            return null;
        }
    }

    /**
     * flink 实时计算价格平均值写入 Redis
     * 窗口间隔为 10s
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<ProductBean> kafkaSource = env.addSource(new CustomizedKafkaConsumer());
        DataStream<Tuple3<String, Integer, BigDecimal>> ds = kafkaSource.flatMap((FlatMapFunction<ProductBean, Tuple3<String, Integer, BigDecimal>>) (in, out) -> {
            if (null != in) {
                out.collect(new Tuple3<>(in.getProductName(), in.getStatus(), in.getPrice()));
            }
        }).returns(new TypeHint<Tuple3<String, Integer, BigDecimal>>() {
        });

        ds.keyBy(tuple -> tuple.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).apply((
                WindowFunction<Tuple3<String, Integer, BigDecimal>,
                        Tuple2<Long, BigDecimal>, String, TimeWindow>) (tuple, window, in, out) -> {
            long cnt = 0L;
            BigDecimal sum = BigDecimal.ZERO;
            for (Tuple3<String, Integer, BigDecimal> record : in) {
                sum = sum.add(record.f2);
                cnt++;
            }
            in.iterator().next();
            out.collect(new Tuple2<>(window.getEnd(), sum.divide(BigDecimal.valueOf(cnt), RoundingMode.CEILING)));
        }).returns(new TypeHint<Tuple2<Long, BigDecimal>>() {
        }).addSink(new RedisSink<>(new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1")
                .setPort(6379).build(), new CustomizedRedisSink()));
        env.execute();
    }
}
