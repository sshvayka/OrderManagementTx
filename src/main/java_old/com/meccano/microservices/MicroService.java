package com.meccano.microservices;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.meccano.Main;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBconfig;
import org.apache.log4j.Logger;

import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 *
 * Base class for simulating the MS behaviour using a thread
 * Use a common KafkaBroker to exchange messages with other MS
 */
public abstract class MicroService implements Runnable {

    protected String type;
    protected UUID instance;
    protected KafkaBroker kafka;
    protected String topic_subscription;
    protected CBconfig db;
    protected boolean finish;

    protected CouchbaseCluster cluster;
    protected Bucket bucket;
    static Logger log = Logger.getLogger(MicroService.class.getName());


    public MicroService(String type, KafkaBroker kafka, String topic, CBconfig db){
        this.type=type;
        this.instance = UUID.randomUUID();
        this.topic_subscription=topic;
        log.debug("Microservice thread created");
        if (db!=null) {
            this.db=db;
            this.finish = false;
        }
        else{
            log.error("[ERROR] MS "+type+" generation: CBconfig is null");
            this.finish=true;
            return;
        }

        if (kafka!=null) {
            this.kafka = kafka;
            this.finish = false;
        }
        else{
            log.error("[ERROR] MS "+type+" generation: Kafka is null");
            this.finish=true;
            return;
        }
        // Use the cluster connection
        cluster = db.cluster;
        // Connect to the bucket and open it
        if (db.password!=null)
            bucket = cluster.openBucket(db.bucket,db.password);
        else
            bucket = cluster.openBucket(db.bucket);

    }
    public String getType(){
        return this.type;
    }
    public String getTopicSubscription(){
        return this.topic_subscription;
    }
    public UUID getInstance() {
        return this.instance;
    }
    public String getID(){
        return type +"-"+instance.toString();
    }

    public void run(){
        KafkaMessage message;
        while (!finish){
            message = consumMessage();
            if(message!=null)
                if (message.getType()=="Kill")
                    this.finish=true;
                else
                    this.processMessage(message);
        }
        exit();
    }
    protected  KafkaMessage consumMessage(){
        return this.kafka.getMessage(this.getTopicSubscription());
    }

    protected abstract void processMessage(KafkaMessage message);
    protected abstract void exit();
}
