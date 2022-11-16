package com.star.consumer;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;

public class CustomizedKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        while (true) {
            Thread.sleep(3000);
            writer();
        }
    }

    private static void writer() {
        try {
            Properties prop = new Properties();
            prop.put("bootstrap.servers", "localhost:9092");
            prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
            ProductBean bean = ProductBean.builder().productName("abc")
                    .status(101)
                    .price(BigDecimal.valueOf(new Random().nextInt(100))).build();
            ProducerRecord<String, String> record = new ProducerRecord<>("test", null, "Bean", JSON.toJSONString(bean));
            producer.send(record);
            System.out.println("send ===");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
