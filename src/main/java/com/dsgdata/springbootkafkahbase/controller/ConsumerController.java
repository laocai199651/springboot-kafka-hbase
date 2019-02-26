package com.dsgdata.springbootkafkahbase.controller;

import com.dsgdata.springbootkafkahbase.service.ConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;


@SuppressWarnings("deprecation")
@Controller
public class ConsumerController {
	private static final Logger logger = LoggerFactory.getLogger(ConsumerController.class);
	@Autowired
	@Qualifier("consumerService")
	private ConsumerService consumerService;
	@RequestMapping(value = "/consumer/toHbase", produces = "application/json; charset=utf-8")
	@ResponseBody
	public Boolean getMsgToHbase(String serviceId,String topic,String ip,String port,String znode,String groupname,String hzookeeper,String hport,String hznode,String description,HttpServletRequest request,HttpServletResponse response) throws ServletException{
		StringBuffer zookeeper = new StringBuffer("");
		if(ip != null && !ip.isEmpty()&&port != null&&!port.isEmpty()){
			String[] ips = ip.split(",");
			for(String i:ips){
				zookeeper.append(i+":"+port+",");
			}
			zookeeper.deleteCharAt(zookeeper.lastIndexOf(","));
			if(znode != null && !znode.isEmpty())
			zookeeper.append(znode);
		}
		System.out.println(zookeeper.toString());
		return consumerService.getMsgToHbase(serviceId,topic, zookeeper.toString(), groupname, hzookeeper, hport,hznode,description);
	}
	/*@RequestMapping(value = "/consumer/toHive", produces = "application/json; charset=utf-8")
	@ResponseBody
	public Boolean getMsgToHive(String serviceId,String topic,String zookeeper,String groupname,String description,HttpServletRequest request,HttpServletResponse response) throws ServletException{
		
		return consumerService.getMsgToHive(serviceId,topic, zookeeper, groupname,description);
	}
	@RequestMapping(value = "/consumer/deleteConsumer", produces = "application/json; charset=utf-8")
	@ResponseBody
	public Integer deleteConsumer(String serviceId,String ip,String port,String znode, String topic, String groupname, String todo,HttpServletRequest request,HttpServletResponse response) throws ServletException{
		StringBuffer zookeeper = new StringBuffer("");
		if(ip != null && !ip.isEmpty()&&port != null&&!port.isEmpty()){
			String[] ips = ip.split(",");
			for(String i:ips){
				zookeeper.append(i+":"+port+",");
			}
			zookeeper.deleteCharAt(zookeeper.lastIndexOf(","));
			if(znode != null && !znode.isEmpty())
			zookeeper.append(znode);
		}
		System.out.println(zookeeper.toString());
		return consumerService.deleteConsumer(serviceId, zookeeper.toString(), topic, groupname, todo);
	}
	@RequestMapping(value = "/consumer/getConsumersInGroup", produces = "application/json; charset=utf-8")
	@ResponseBody
	public List<String> getConsumersInGroup(String ip,String port,String znode,String groupname,HttpServletRequest request,HttpServletResponse response) throws ServletException{
		StringBuffer zookeeper = new StringBuffer("");
		if(ip != null && !ip.isEmpty()&&port != null&&!port.isEmpty()){
			String[] ips = ip.split(",");
			for(String i:ips){
				zookeeper.append(i+":"+port+",");
			}
			zookeeper.deleteCharAt(zookeeper.lastIndexOf(","));
			if(znode != null && !znode.isEmpty())
			zookeeper.append(znode);
		}
		System.out.println(zookeeper.toString());
		return consumerService.getConsumersInGroup(zookeeper.toString(), groupname);
	}*/
	@RequestMapping(value = "/consumer/getConsumers", produces = "application/json; charset=utf-8")
	@ResponseBody
	public List<Map<String, String>> getConsumers(String serviceId, String zookeeper, String topic, String groupname, String todo,HttpServletRequest request,HttpServletResponse response) throws ServletException{
		/*try {
			return ConsumerUtil.selectConsumer(serviceId, zookeeper, topic, groupname, todo);
		} catch (Exception e) {
			e.printStackTrace();
			throw new LongGangKafkaRuntimeException("查询consumer失败",e);
		}*/

		return null;
	}

}
