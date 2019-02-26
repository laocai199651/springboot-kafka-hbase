package com.dsgdata.springbootkafkahbase.consumer;

import com.alibaba.fastjson.JSON;
import com.dsgdata.springbootkafkahbase.disrupter.KafkaEventProducer;
import com.dsgdata.springbootkafkahbase.bean.Kafka_RealSync_Event;
import com.dsgdata.springbootkafkahbase.disrupter.KafkaEvent;
import com.dsgdata.springbootkafkahbase.utils.HbaseUtils;
import com.lmax.disruptor.RingBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Consumer2Hbase extends Consumer implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(Consumer2Hbase.class);


    public Consumer2Hbase(Properties props, String topic, long offset, int... partitions) {
        super(props, topic, partitions, offset);
    }

    @Override
    public void run() {
        if (offset == -1) {
            kafkaMsg2HbaseInitial(topic, partitions);
        } else {
            kafkaMsg2Hbase(topic, partitions[0], offset);
        }
    }

    /**
     * @param topic
     * @param partitions
     */
    @Override
    public void kafkaMsg2HbaseInitial(String topic, int... partitions) {

        AtomicBoolean isTableExists = new AtomicBoolean(false);

        this.consumer = KafkaConsumerGenerator.GetKafkaConsumer(props);

        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();

        for (int partition : partitions) {
            topicPartitions.add(new TopicPartition(topic, partition));
        }
        consumer.assign(topicPartitions);
        //consumer.seekToBeginning(topicPartitions);
        final int minBatchSize = 200;
        //consumer.commitSync(Collections.singletonMap(partition0,new OffsetAndMetadata(0)));
        //List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        RingBuffer<KafkaEvent> ringBuffer = HbaseUtils.getIIDisruptor(topic);
        KafkaEventProducer kafkaEventProducer = new KafkaEventProducer(ringBuffer);
        //建表
        synchronized (String.class) {
            try {
                //isTableExists.set();
                if (!HbaseUtils.tableExist("TABLE_" + topic)) {
                    HbaseUtils.createTable("TABLE_" + topic, "COLUMN");
                    logger.info("已建表\t" + "TABLE_" + topic);
                }
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }
        while (!closed.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                long position = consumer.position(topicPartitions.get(0));
                //System.out.println("position: "+position);
                if (records.count() > 0) {
                    System.out.println("records.count" + records.count());
                }
                for (ConsumerRecord<String, String> record : records) {
                    /* buffer.add(record);*/
                    //System.out.println("record: " + record.value() + " offset: " + "\n" + record.offset() + "\n" + record.partition());
                   /* System.out.println(" offset: " + "\n" + record.offset() + "\n" + record.partition());
                    if (record.offset() < 30) {
                        consumer.commitSync();
                        System.out.println("commit: " + record.offset());
                    }*/
                    Kafka_RealSync_Event kafkaRealSyncEvent = JSON.parseObject(record.value(), Kafka_RealSync_Event.class);
                    //kafkaRealSyncEvent.updateMetadata();
                    //kafkaRealSyncEvent.updateMetadataMap();
                    kafkaRealSyncEvent.setTimestamp(record.timestamp());
                    kafkaEventProducer.onData(kafkaRealSyncEvent, record);
                    //consumer.commitSync(Collections.singletonMap(topicPartitions.get(0), new OffsetAndMetadata(record.offset() + 1)));
                }
            } catch (Exception e) {
                e.printStackTrace();
                this.consumer.pause(topicPartitions);
            } finally {
            }

          /*  if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }*/
        }

    }

    /**
     * @param topic
     * @param partition
     * @param offset
     */
    @Override
    public void kafkaMsg2Hbase(String topic, int partition, long offset) {

        this.consumer = KafkaConsumerGenerator.GetKafkaConsumer(props);
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition, offset);
        final int minBatchSize = 200;
        //consumer.commitSync(Collections.singletonMap(partition0,new OffsetAndMetadata(0)));
        //List<ConsumerRecord<String, String>> buffer = new ArrayList<>();

          /*  if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }*/
        LinkedList<Object> list = new LinkedList<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                if (records.count() > 0)
                    System.out.println("records.count" + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    /* buffer.add(record);*/

                   /* Kafka_RealSync_Event event = JSON.parseObject(record.value(), Kafka_RealSync_Event.class);
                    event.setTimestamp(record.timestamp());
                    event.updateMetadata();
                    event.updateMetadataMap();
                    list.add(event);*/
                    System.out.println("record: " + record.value() + " offset: " + record.offset());
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            this.consumer.pause(Arrays.asList(topicPartition));
        } finally {
        }
    }


}


