package com.cwk.springbootkafkahbase.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class NewKafkaConsumerManualOffsetAndPartion {

   private Properties props = new Properties();

    public NewKafkaConsumerManualOffsetAndPartion(Properties props) {
        this.props = props;
    }

    public void KafkaMsg2Hbase(String topic){
       //Properties props = new Properties();
       //props.put("bootstrap.servers", "192.168.23.53:9092");
      /* props.put("bootstrap.servers", "192.168.23.123:9092");
       props.put("group.id", "test1");
       props.put("enable.auto.commit", "false");
       props.put("max.poll.records", 1);
       props.put("auto.offset.reset","latest");
       props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
       props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
*/
      KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

     TopicPartition partition0 = new TopicPartition(topic, 0);
//       TopicPartition partition1 = new TopicPartition(topic, 1);
       consumer.assign(Arrays.asList(partition0/*, partition1*/));

       //consumer.subscribe(Arrays.asList(topic));

       final int minBatchSize = 200;
       //consumer.commitSync(Collections.singletonMap(partition0,new OffsetAndMetadata(0)));
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



}
