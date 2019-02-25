package com.cwk.springbootkafkahbase;

import com.cwk.springbootkafkahbase.consumer.Consumer2Hbase;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DemoTest {

    @Test
    public void test01() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.9.97:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            String data = "{\"batchID\":\"" + System.currentTimeMillis() + "\",\"batchRowCount\":1,\"columnListInfo\":[{\"btype\":15,\"cflag\":64,\"colNum\":0,\"columnName\":\"ID\",\"columnValue\":\"13\",\"dataColNum\":0,\"len\":2,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":0,\"colNum\":1,\"columnName\":\"NAME\",\"columnValue\":\"a13\",\"dataColNum\":1,\"len\":3,\"oraType\":[0,9],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":2,\"columnName\":\"scn_code\",\"columnValue\":\"17398172\",\"dataColNum\":-1,\"len\":8,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":3,\"columnName\":\"scn_time\",\"columnValue\":\"2019-02-15 13:23:32\",\"dataColNum\":-1,\"len\":19,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":4,\"columnName\":\"loadtime\",\"columnValue\":\"2019-02-15 13:23:42\",\"dataColNum\":-1,\"len\":19,\"oraType\":[0,12],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":5,\"columnName\":\"trans_id\",\"columnValue\":\"1688978709293049\",\"dataColNum\":-1,\"len\":16,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":6,\"columnName\":\"seq_id\",\"columnValue\":\"1\",\"dataColNum\":-1,\"len\":1,\"oraType\":[0,2],\"otype\":0,\"rowID\":\"\",\"rowNum\":0},{\"btype\":15,\"cflag\":4,\"colNum\":7,\"columnName\":\"ds_rowid\",\"columnValue\":\"AAASSvAAEAAAAlVAAF\",\"dataColNum\":-1,\"len\":18,\"oraType\":[0,1],\"otype\":0,\"rowID\":\"\",\"rowNum\":0}],\"columnNum\":7,\"operation_type\":\"I\",\"owner\":\"X1\",\"rowNum\":0,\"tableName\":\"TEST001\"}";
            producer.send(new ProducerRecord<String, String>("topic-json-test", data));
        }

        producer.close();
    }

    @Resource(name = "getKafkaProperties")
    Properties pops;

    @Test
    public void test02() {

        KafkaConsumer consumer = new KafkaConsumer(pops);

        Map<String, List<PartitionInfo>> listTopics = consumer.listTopics();

        /*for (Map.Entry<String,List<PartitionInfo>> o : listTopics.entrySet()) {
            System.out.println(o.getKey()+"\t"+o.getValue());
        }*/
        //获取指定topic的分区metadata
        List<PartitionInfo> partitionInfos = listTopics.get("topic-json-test");

        /* Partition(topic = topic-json-test, partition = 2, leader = 0, replicas = [2,0,1], isr = [0,1,2], offlineReplicas = [])
            Partition(topic = topic-json-test, partition = 1, leader = 2, replicas = [0,1,2], isr = [2,1,0], offlineReplicas = [])
            Partition(topic = topic-json-test, partition = 0, leader = 0, replicas = [1,2,0], isr = [0,1,2], offlineReplicas = [])*/
        for (PartitionInfo partitionInfo : partitionInfos) {
            System.out.println(partitionInfo);


        }
        /*//主题元数据信息请求
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList("topic-json-test"));
        //获取主题元数据返回值
        TopicMetadataResponse topicMetadataResponse = consumer.send(topicMetadataRequest);
        //解析返回值
        List<TopicMetadata> topicMetadata = topicMetadataResponse.topicsMetadata();
        for (TopicMetadata topicMetadatum : topicMetadata) {
            //获取多个分区的元数据信息
            List<PartitionMetadata> partitionMetadata = topicMetadatum.partitionsMetadata();
            for (PartitionMetadata partitionMetadatum : partitionMetadata) {
                System.out.println( partitionMetadatum.partitionId());
            }
        }*/

    }

    @Test
    public void test03() throws InterruptedException {

        Properties prop = pops;

        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "test-json-05");

        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //这里为什么max.poll数量为一时，手动commit才起作用？
        //Commit offsets returned on the last poll() for all the subscribed list of topics and partitions.
        //prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        KafkaConsumer consumer = new KafkaConsumer(prop);

        Map<String, List<PartitionInfo>> listTopics = consumer.listTopics();

        /*for (Map.Entry<String,List<PartitionInfo>> o : listTopics.entrySet()) {
            System.out.println(o.getKey()+"\t"+o.getValue());
        }*/
        String topic = "topic-json-test";
        //获取指定topic的分区metadata
        List<PartitionInfo> partitionInfos = listTopics.get(topic);

        ExecutorService service = Executors.newFixedThreadPool(partitionInfos.size());

        HashMap<String, Thread> threadHashMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            Consumer2Hbase consumer2Hbase = new Consumer2Hbase(pops, topic, -1,partitionInfo.partition() );
            threadHashMap.put(partitionInfo.topic() + "-" + partitionInfo.partition(), consumer2Hbase);
            service.execute(consumer2Hbase);
        }
        service.shutdown();

        Thread.sleep(20000);

    }


}
