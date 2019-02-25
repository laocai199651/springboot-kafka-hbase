package com.cwk.springbootkafkahbase.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class Consumer2Hbase extends Consumer implements Runnable {

    private Consumer2Hbase(Properties props) {
        super(props);
    }

    @Override
    public void run() {

    }

    /**
     *
     * @param topic
     * @param partition
     */
    @Override
    public void kafkaMsg2HbaseInitial(String topic,int partition) {


        this.consumer = new KafkaConsumer<>(props);

        TopicPartition partition0 = new TopicPartition(topic, 0);
//       TopicPartition partition1 = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partition0));

        //consumer.subscribe(Arrays.asList(topic));

        final int minBatchSize = 200;
        consumer.commitSync(Collections.singletonMap(partition0,new OffsetAndMetadata(0)));
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            if (records.count()>0)
                System.out.println("records.count"+records.count());
            for (ConsumerRecord<String, String> record : records) {
                /* buffer.add(record);*/
                System.out.println("record:"+record.offset());
                System.out.println(record.value());
                if (record.offset()<=20)
                {
                    System.out.println("commit");
                    consumer.commitSync();
                }
            }
          /*  if (buffer.size() >= minBatchSize) {
                //insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }*/
        }

    }

    /**
     *
     * @param topic
     * @param partition
     * @param offset
     */
    @Override
    public void kafkaMsg2Hbase(String topic, int partition, int offset) {

    }

    /**
     *
     * @param id
     * @return
     */
    public Consumer getConsumer(String id){
        Consumer consumer=null;




        return consumer;
    }

}
