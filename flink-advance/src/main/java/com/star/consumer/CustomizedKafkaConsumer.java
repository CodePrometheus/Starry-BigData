package com.star.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 自定义 Kafka 消费
 */
public class CustomizedKafkaConsumer extends RichSourceFunction<ProductBean> {
    private final static Logger logger = LoggerFactory.getLogger(CustomizedKafkaConsumer.class);
    KafkaConsumer<String, String> consumer;

    @Override
    public void open(Configuration parameters) {
        Properties props = new Properties();
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test01");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 创建消费者
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test"));
        System.out.println("consumer = " + JSON.toJSONString(consumer));
    }

    /**
     * Kafka 消费输入的数据
     *
     * @param ctx /
     */
    @Override
    public void run(SourceContext<ProductBean> ctx) {
        System.out.println("run====");
        while (true) {
            // 拉取一段时间内的数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(3));
            System.out.println("records = " + JSON.toJSON(records));
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                System.out.println("record.value = " + value);
                ProductBean product = JSON.parseObject(value, ProductBean.class);
                System.out.println("product = " + product);
                ctx.collect(product); // 与下游输出或者是 sink 类型必须一致
            }
        }
    }

    @Override
    public void cancel() {
    }
}
