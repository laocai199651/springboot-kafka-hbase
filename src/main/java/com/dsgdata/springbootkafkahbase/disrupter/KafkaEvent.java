package com.dsgdata.springbootkafkahbase.disrupter;

import com.dsgdata.springbootkafkahbase.bean.Kafka_RealSync_Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaEvent {

        private Kafka_RealSync_Event kafkaRealSyncEvent;

        private ConsumerRecord record;

    public Kafka_RealSync_Event getKafkaRealSyncEvent() {
        return kafkaRealSyncEvent;
    }

    public KafkaEvent() {
    }

    public void setKafkaRealSyncEvent(Kafka_RealSync_Event kafkaRealSyncEvent) {
        this.kafkaRealSyncEvent = kafkaRealSyncEvent;
    }

    public ConsumerRecord getRecord() {
        return record;
    }

    public void setRecord(ConsumerRecord record) {
        this.record = record;
    }

    public KafkaEvent(Kafka_RealSync_Event kafkaRealSyncEvent, ConsumerRecord record) {
        this.kafkaRealSyncEvent = kafkaRealSyncEvent;
        this.record = record;
    }
}
