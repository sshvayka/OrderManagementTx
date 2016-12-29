package com.meccano.kafka;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 *
 * A kafka broker is composed of >0 independent topics.
 * A topic is represented using a ConcurrentLinkedQueue
 */
public class KafkaTopic {

    protected ConcurrentLinkedQueue<KafkaMessage> topic;
    protected String name;

    protected KafkaTopic (String name){
        this.name=name;
        topic = new ConcurrentLinkedQueue<KafkaMessage> ();
    }
    protected String getName(){
        return this.name;
    }

    protected void put (KafkaMessage msg){
        topic.add(msg);
    }

    protected KafkaMessage get(){
        return topic.poll();
    }

    protected int size(){
        return topic.size();
    }
}
