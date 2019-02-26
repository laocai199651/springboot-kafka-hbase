package com.dsgdata.springbootkafkahbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringbootKafkaHbaseApplication {

    public static void main(String[] args) throws ClassNotFoundException {

        SpringApplication.run(SpringbootKafkaHbaseApplication.class, args);
     /*   Object properties = context.getBean("getKafkaProperties");

        properties.hashCode();*/

    }


}
