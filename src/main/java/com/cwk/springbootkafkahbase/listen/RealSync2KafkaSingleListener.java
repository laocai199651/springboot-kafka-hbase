package com.cwk.springbootkafkahbase.listen;

import com.alibaba.fastjson.JSON;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Event;
import com.cwk.springbootkafkahbase.utils.HbaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class RealSync2KafkaSingleListener {

//    private static final Logger log = LoggerFactory.getLogger(RealSync2KafkaSingleListener.class);
//
//    @KafkaListener(id = "topic-dsg003-consumer", topics = {"topic.cwkkafka2HbaseTest"})
//    public void batchListenerWithPartition(String data) throws Exception {
//        log.info("topic-dsg003 consumer  receive data time:" + System.currentTimeMillis());
//
//        //检查表
//        if (!HbaseUtils.tableExist("kafka2HbaseEventTest")) {
//            HbaseUtils.createTable("kafka2HbaseEventTest", "column");
//        }
//        LinkedList<Object> list = new LinkedList<>();
//        Kafka_RealSync_Event event = JSON.parseObject(data, Kafka_RealSync_Event.class);
//        event.updateMetadata();
//        event.updateMetadataMap();
//        list.add(event);
//        HbaseUtils.putRealSyncDataBatch("kafka2HbaseEventTest", list, System.currentTimeMillis(), "column");
//
//    }

}
