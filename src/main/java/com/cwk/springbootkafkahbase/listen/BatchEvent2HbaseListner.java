package com.cwk.springbootkafkahbase.listen;

import com.alibaba.fastjson.JSON;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Event;
import com.cwk.springbootkafkahbase.utils.HbaseUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

@Component
public class BatchEvent2HbaseListner {

//    private static final Logger log = LoggerFactory.getLogger(BatchEvent2HbaseListner.class);
//
//    //@KafkaListener(id = "topic-dsg003-consumer", topics = "topic-dsg003",containerFactory = "batchContainerFactory")
//    @KafkaListener(id = "topic-dsg003-consumer", clientIdPrefix = "topic-dsg003-consumer", containerFactory = "batchContainerFactory", topics = "topic-dsg003",
//            topicPartitions = {
//                    @TopicPartition(topic = "topic-dsg003",
//                            partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "5"))
//            }
//    )
//    public void batchListenerWithPartition(List<ConsumerRecord<Integer, String>> datas) throws Exception {
//        log.info("topic-dsg003 consumer  receive data time:" + System.currentTimeMillis());
//
//        //检查表
//        if (!HbaseUtils.tableExist("kafka2HbaseEventTest")) {
//            HbaseUtils.createTable("kafka2HbaseEventTest", "column");
//        }
//        LinkedList<Object> list = new LinkedList<>();
//        for (ConsumerRecord<Integer, String> data : datas) {
//            Kafka_RealSync_Event event = JSON.parseObject(data.value(), Kafka_RealSync_Event.class);
//            event.setTimestamp(data.timestamp());
//            event.updateMetadata();
//            event.updateMetadataMap();
//            list.add(event);
//        }
//        //UUID.randomUUID().toString().replaceAll("-", "")
//        //UUID.randomUUID().toString().replaceAll("-", "")
//        HbaseUtils.putRealSyncDataBatch("kafka2HbaseEventTest", list, "column");
//
//    }

}
