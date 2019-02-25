package com.cwk.springbootkafkahbase.consumer;

import com.alibaba.fastjson.JSON;
import com.cwk.springbootkafkahbase.bean.Kafka_RealSync_Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class Consumer2Hbase extends Consumer implements Runnable {

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
        while (!closed.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                long position = consumer.position(topicPartitions.get(0));
                //System.out.println("position: "+position);
                if (records.count() > 0)
                    System.out.println("records.count" + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    /* buffer.add(record);*/
                    //System.out.println("record: " + record.value() + " offset: " + "\n" + record.offset() + "\n" + record.partition());
                    System.out.println(" offset: " + "\n" + record.offset() + "\n" + record.partition());
                    if (record.offset() < 30) {
                        //consumer.commitSync();
                        consumer.commitSync(Collections.singletonMap(topicPartitions.get(0), new OffsetAndMetadata(record.offset()+1)));
                        System.out.println("commit: " + record.offset());
                    }
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


