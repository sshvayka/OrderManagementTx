package com.meccano.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 * Simulates the behaviour of a Kakfa cluster with multiple topics.
 *
 */
public class KafkaBroker {

    private ArrayList<KafkaTopic> topics;
    private static Logger log = LogManager.getLogger(KafkaBroker.class);

    public KafkaBroker(){
        topics = new ArrayList<>();
    }

    public KafkaBroker(List<String> names){
        for (String name : names) {
            topics.add(new KafkaTopic(name));
        }
    }

    public void createTopic (String topicName){
        boolean exist = false;
        for (KafkaTopic topic : topics){
            if (topic.getName().equals(topicName)){
                exist = true;
                break;
            }
        }
        if (!exist)
            topics.add(new KafkaTopic(topicName));
    }

    public ArrayList<String> getTopicsName(){
        ArrayList<String> names = new ArrayList<>();
        for (KafkaTopic topic : topics) {
            names.add(topic.getName());
        }
        return names;
    }

    public KafkaTopic getTopic(String topicName){
        KafkaTopic topicFound = null;
        for(KafkaTopic topic : topics){
            if (topic.getName().equals(topicName)){
                topicFound = topic;
                break;
            }
        }
        return topicFound;
    }

    public KafkaMessage getMessage(String topicName){
        KafkaTopic topic = this.getTopic(topicName);
        KafkaMessage msg = null;
        if (topic != null){
            if (topic.size() > 0)
                msg = topic.get();
        } else {
            log.error("[ERROR] KakfaBroker - " + topicName + " KafkaTopic is null");
        }
        return msg;
    }

    public void putMessage(String topicName, KafkaMessage msg){
        KafkaTopic topic = this.getTopic(topicName);
        if (topic != null)
            topic.put(msg);
    }

    public void removeTopic(String topicName){
        Iterator<KafkaTopic> itr = topics.iterator();
        KafkaTopic topic;
        while (itr.hasNext()) {
            topic = itr.next();
            if (topic.getName().equals(topicName)) {
                itr.remove();
                break;
            }
        }
    }

    public int topicSize(String topicName){
        KafkaTopic topic = this.getTopic(topicName);
        int size = -1;
        if (topic != null)
            size = topic.size();
        return size;
    }

    public int totalSize(){
        int total = 0;
        for(KafkaTopic topic : topics){
            total += topic.size();
        }
        return total;
    }
}