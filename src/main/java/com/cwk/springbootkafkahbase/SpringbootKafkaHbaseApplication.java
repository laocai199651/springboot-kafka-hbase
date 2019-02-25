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

            //消费者配置参数
            Properties props = new Properties();
            //连接地址
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getServers().get(1) + ":" + properties.getServerPort());
            //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.23.121:9092");
            //GroupID
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-01");

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,properties.getAutoOffSetReset());
            //是否自动提交
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            //自动提交的频率
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
            //Session超时设置
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
            //键的反序列化方式
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getKeyDeserializer()));
            //值的反序列化方式
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(properties.getValueDeserializer()));


        NewKafkaConsumerManualOffsetAndPartion newKafkaConsumerManualOffsetAndPartion = new NewKafkaConsumerManualOffsetAndPartion(props);
        newKafkaConsumerManualOffsetAndPartion.KafkaMsg2Hbase("topic-quick-ack-1");
    }

}
