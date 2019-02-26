package com.dsgdata.springbootkafkahbase;

import com.dsgdata.springbootkafkahbase.consumer.Consumer2Hbase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class VoidRun {

/*

    @Autowired
    ApplicationContext context;


    @Bean
    public Void run() {
        //ApplicationContext context = SpringApplication.run(SpringbootKafkaHbaseApplication.class, args);
        //ApplicationContext context = SpingTool.getApplicationContext();

//        SpringApplication application = new SpringApplication(SpringbootKafkaHbaseApplication.class);
//        ConfigurableApplicationContext context = application.run(args);
        Properties prop = (Properties) context.getBean("getKafkaProperties");

        prop.put(ConsumerConfig.GROUP_ID_CONFIG, "test-json-07");

        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //这里为什么max.poll数量为一时，手动commit才起作用？
        //Commit offsets returned on the last poll() for all the subscribed list of topics and partitions.
        //prop.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        KafkaConsumer consumer = new KafkaConsumer(prop);

        Map<String, List<PartitionInfo>> listTopics = consumer.listTopics();

        */
/*for (Map.Entry<String,List<PartitionInfo>> o : listTopics.entrySet()) {
            System.out.println(o.getKey()+"\t"+o.getValue());
        }*//*

        String topic = "topic-json-test";
        //获取指定topic的分区metadata
        List<PartitionInfo> partitionInfos = listTopics.get(topic);

        ExecutorService service = Executors.newFixedThreadPool(partitionInfos.size());

        HashMap<String, Thread> threadHashMap = new HashMap<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            Consumer2Hbase consumer2Hbase = new Consumer2Hbase(prop, topic, -1, partitionInfo.partition());
            threadHashMap.put(partitionInfo.topic() + "-" + partitionInfo.partition(), consumer2Hbase);
            service.execute(consumer2Hbase);
        }
        service.shutdown();
        return null;
    }
*/

}
