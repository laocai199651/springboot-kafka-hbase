package com.dsgdata.springbootkafkahbase.service;

import com.dsgdata.springbootkafkahbase.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("consumerService")
public class ConsumerService {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);
	private static Map<String, Consumer> consumers = new HashMap<String,Consumer>();
	public synchronized Boolean getMsgToHbase(String id,String topic,String zookeeper,String groupId,String hzookeeper,String hport,String hznode,String description){
		/*if(id == null || id.isEmpty()){
			throw new LongGangKafkaValidationException("请传入服务id");
		}
		if(topic == null || topic.isEmpty()){
			throw new LongGangKafkaValidationException("请传入topic，即想要移动哪个topic里面的数据");
		}
		if(groupId == null || groupId.isEmpty()){
			throw new LongGangKafkaValidationException("请传入groupname,即创建的consumer属于哪个group，如果group不存在会自动创建");
		}
		if(zookeeper == null || zookeeper.isEmpty()){
			zookeeper = Constants.errorMsgZookeeper;
		}
		if(hzookeeper == null || hzookeeper.isEmpty()){
			hzookeeper = Constants.hbasezk;
		}
		if(hport == null || hport.isEmpty()){
			hport = Constants.hbaseport;
		}
		try {
			List<Map<String,String>> consumer = ConsumerUtil.selectConsumer(id, "", "", "", "");
			if(consumer.size()>0){//consumer已经存在
				return true;
			}
		} catch (Exception e) {
		}
		int consumernum = 0;
		try{
			List<String> consumernames1 = getConsumersInGroup(zookeeper, groupId);
			consumernum = consumernames1.size();
		} catch (ZkNoNodeException e) {
//			e.printStackTrace();
		}
//		PartitionService partitionservice = new PartitionService();
//		Integer pnum = partitionservice.partitionNum(zookeeper, topic);//分区数目
		Consumer consumer = new ConsumerToHbase(id,topic,zookeeper,groupId,hzookeeper,hport,hznode);  
		consumer.start(); 
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
		}
		try {
			ConsumerUtil.addConsumer(id, zookeeper, topic, groupId, "toHbase",description,"");
		} catch (Exception e1) {
			logger.error("fail to add consumer msg into sqlite",e1);
			consumer.shutdownConnection();
			return false;
		}
		consumers.put(id, consumer);
//		for(int i = 0;i<pnum;i++){
//		}
	
		List<String> consumernames2 = getConsumersInGroup(zookeeper, groupId);
		if(consumernames2.size() - consumernum == 1){
			return true;
		}else{
			return false;
		}*/
		return null;
	}

}
