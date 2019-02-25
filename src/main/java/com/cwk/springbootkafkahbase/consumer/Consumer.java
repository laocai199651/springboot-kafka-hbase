package com.cwk.springbootkafkahbase.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Consumer extends Thread {

    protected Properties props = new Properties();

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected KafkaConsumer consumer=null;


    public Consumer(Properties props) {
        this.props = props;
    }

    public abstract void kafkaMsg2HbaseInitial(String topic,int partition);

    public abstract void kafkaMsg2Hbase(String topic,int partition,int offset);
}
