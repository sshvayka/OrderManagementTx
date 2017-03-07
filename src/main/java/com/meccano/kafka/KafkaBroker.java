package com.meccano.kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 *
 * Simulates the behaviour of a Kakfa cluster with multiple topics.
 */
public class KafkaBroker {

    protected ArrayList<KafkaTopic> topics;
    private static Logger log = LogManager.getLogger(KafkaBroker.class.getName());

    public KafkaBroker(){
        topics = new ArrayList<KafkaTopic>();
    }

    public KafkaBroker(List<String> names){
        Iterator<String> itr = names.iterator();
        while (itr.hasNext()){
            topics.add(new KafkaTopic(itr.next()));
        }
    }

    public void createTopic (String topic_name){
        boolean exist= false;
        KafkaTopic topic;
        Iterator<KafkaTopic> itr = topics.iterator();
        while (itr.hasNext()) {
            topic = itr.next();
            if (topic.getName().equals(topic_name))
                exist = true;
        }
        if (!exist)
            topics.add(new KafkaTopic(topic_name));
    }

    public ArrayList<String> getTopicsName(){
        ArrayList<String> names = new ArrayList<String>();
        Iterator<KafkaTopic> itr = topics.iterator();
        while (itr.hasNext()){
            names.add(itr.next().getName());
        }
        return names;
    }

    protected KafkaTopic getTopic(String topic_name){
        Iterator<KafkaTopic> itr = topics.iterator();
        KafkaTopic topic = null;
        while (itr.hasNext()) {
            topic = itr.next();
            if (topic.getName().equals(topic_name))
                return topic;
        }
        return topic;
    }

    public KafkaMessage getMessage(String topic_name){
        KafkaTopic topic = this.getTopic(topic_name);
        if (topic != null){
            if (topic.size() > 0)
                return topic.get();
            else
                return null;
        } else {
            log.error("[ERROR] KakfaBroker - "+ topic_name+" KakfaTopic is null");
            return null;
        }
    }

    public void putMessage(String topic_name, KafkaMessage msg){
        KafkaTopic topic = this.getTopic(topic_name);
        if (topic != null)
            topic.put(msg);
    }

    public void removeTopic(String topic_name){
        Iterator<KafkaTopic> itr = topics.iterator();
        KafkaTopic topic;
        while (itr.hasNext()) {
            topic = itr.next();
            if (topic.getName().equals(topic_name)) {
                itr.remove();
                break;
            }
        }
    }

    public int topicSize(String topic_name){
        KafkaTopic topic = this.getTopic(topic_name);
        int size = -1;
        if (topic != null)
            size = topic.size();
        return size;
    }

    public int totalSize(){
        int total = 0;
        Iterator<KafkaTopic> itr = topics.iterator();
        KafkaTopic topic;
        while (itr.hasNext()) {
            topic = itr.next();
            total += topic.size();
        }
        return total;
    }
}