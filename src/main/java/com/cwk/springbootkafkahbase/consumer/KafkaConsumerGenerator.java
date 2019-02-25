package com.cwk.springbootkafkahbase.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaConsumerGenerator {

    private static final ThreadLocal<KafkaConsumer> LOCAL = new ThreadLocal<KafkaConsumer>();

    public static KafkaConsumer GetKafkaConsumer(Properties props) {

        if (LOCAL.get() == null)
            LOCAL.set(new KafkaConsumer(props));
        return LOCAL.get();
    }


}
