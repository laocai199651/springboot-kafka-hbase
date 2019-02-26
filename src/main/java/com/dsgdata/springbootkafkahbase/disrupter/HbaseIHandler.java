package com.dsgdata.springbootkafkahbase.disrupter;

import com.dsgdata.springbootkafkahbase.utils.HbaseUtils;
import com.lmax.disruptor.WorkHandler;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class HbaseIHandler implements WorkHandler<KafkaEvent> {

    private String consumerId;

    private static AtomicInteger count = new AtomicInteger(0);

    public HbaseIHandler(String consumerId){
        this.consumerId = consumerId;
    }
    @Override
    public void onEvent(KafkaEvent event) throws Exception {
        HbaseUtils.putRealSyncDataBatch(consumerId, new LinkedList<>(Collections.singletonList(event.getKafkaRealSyncEvent())),"COLUMN");
        count.incrementAndGet();
    }

    public int getCount(){
        return count.get();
    }

}
