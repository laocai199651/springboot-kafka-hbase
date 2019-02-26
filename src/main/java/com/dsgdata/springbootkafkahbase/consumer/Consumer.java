package com.dsgdata.springbootkafkahbase.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class Consumer extends Thread {

    protected Properties props = new Properties();

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    protected KafkaConsumer consumer = null;

    protected String topic;

    protected int[] partitions;

    protected Long offset;

    protected AtomicLong consumeNum;

    public Consumer(Properties props, String topic, int[] partitions, long offset) {
        this.props = props;
        this.topic = topic;
        this.partitions = partitions;
        this.offset = offset;
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    public void startup(){
        closed.set(false);
        consumer.resume(consumer.paused());
    }

    public abstract void kafkaMsg2HbaseInitial(String topic, int... partition);

    public abstract void kafkaMsg2Hbase(String topic, int partition, long offset);
}
