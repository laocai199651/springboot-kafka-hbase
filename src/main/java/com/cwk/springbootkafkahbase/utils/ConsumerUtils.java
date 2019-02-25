package com.cwk.springbootkafkahbase.utils;

import com.cwk.springbootkafkahbase.consumer.Consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerUtils {


    private static ExecutorService service = Executors.newCachedThreadPool();


    public void saveConsumer(Consumer consumer){

        service.execute(consumer);
        //service.
    }




}
