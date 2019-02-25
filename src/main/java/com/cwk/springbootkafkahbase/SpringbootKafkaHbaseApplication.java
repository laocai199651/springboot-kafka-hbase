package com.cwk.springbootkafkahbase;

import com.cwk.springbootkafkahbase.config.KafkaProperties;
import com.cwk.springbootkafkahbase.consumer.NewKafkaConsumerManualOffsetAndPartion;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@SpringBootApplication
public class SpringbootKafkaHbaseApplication {




   static KafkaProperties properties;

    public KafkaProperties getProperties() {
        return properties;
    }
    @Autowired
    public void setProperties(KafkaProperties properties) {
        this.properties = properties;
    }

    public static void main(String[] args) throws ClassNotFoundException {
            SpringApplication.run(SpringbootKafkaHbaseApplication.class, args);


    }

}
