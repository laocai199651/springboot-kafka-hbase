package com.dsgdata.springbootkafkahbase.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;


@Configuration
@ConfigurationProperties(prefix = "spring.kafka.consumer")
public class KafkaProperties {
    @Value("${spring.kafka.consumer.key-deserializer}")
    private String keyDeserializer;
    @Value("${spring.kafka.consumer.value-deserializer}")
    private String valueDeserializer;
    private List<String> servers;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffSetReset;
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private boolean enableAutoCommit;
    @Value("${spring.kafka.consumer.auto-commit-interval}")
    private String autoCommitInterval;
    @Value("${spring.kafka.session.timeout.ms}")
    private String sessionTimeoutMs;
    @Value("${spring.kafka.consumer.bootstrap-server-port}")
    private String serverPort;

    public List<String> getServers() {
        return servers;
    }

    public void setServers(List<String> servers) {
        this.servers = servers;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }


    public String getAutoOffSetReset() {
        return autoOffSetReset;
    }

    public void setAutoOffSetReset(String autoOffSetReset) {
        this.autoOffSetReset = autoOffSetReset;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public String getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(String autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public String getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(String sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    @Bean(name = "getKafkaProperties")
    public Properties getKafkaProperties() throws ClassNotFoundException {
        Properties props=new Properties();
        //连接地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,getServers().get((int)Math.round(Math.random()*2))+":"+getServerPort());
        //GroupID
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-01");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,getAutoOffSetReset());
        //是否自动提交
        //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //自动提交的频率
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, getAutoCommitInterval());
        //Session超时设置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, getSessionTimeoutMs());
        //键的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(getKeyDeserializer()));
        //值的反序列化方式
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(getValueDeserializer()));
        return  props;
    }
}
