package com.meccano.microservices;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Base class for simulating the MS behaviour using a thread
 * Use a common KafkaBroker to exchange messages with other MS
 */
public abstract class MicroService implements Runnable {

    private String type;
    private UUID instance;
    private KafkaBroker kafka;
    private String topic_subscription;
    private CBConfig db;
    private boolean finish;

    // Couchbase variables
    private Cluster cluster;
    private Bucket bucket;

    // Logger Log4J2
    private static Logger log = LogManager.getLogger(MicroService.class);


    public MicroService(String type, KafkaBroker kafka, String topic, CBConfig db){
        this.type = type;
        this.instance = UUID.randomUUID();
        this.topic_subscription = topic;

        log.info("Microservice thread created");

        if (db != null) {
            this.db = db;
            this.finish = false;
        } else {
            log.error("[ERROR] MS " + type + " generation: CBConfig is null");
            this.finish = true;
            return;
        }
        if (kafka != null) {
            this.kafka = kafka;
            this.finish = false;
        } else {
            log.error("[ERROR] MS " + type + " generation: Kafka is null");
            this.finish = true;
            return;
        }
        // Use the cluster connection
        cluster = db.getCluster();
        // Connect to the bucket and open it
        if (db.getPassword() != null)
            bucket = cluster.openBucket(db.getBucket(), db.getPassword());
        else
            bucket = cluster.openBucket(db.getBucket());
    }

    public void run(){
        KafkaMessage message;
        while (!finish){
            message = consumMessage();
            if(message != null)
                if (message.getType() == "Kill")
                    this.finish = true;
                else
                    this.processMessage(message);
        }
        exit();
    }

    // Define the set of stores associated to this MS instance
    protected ArrayList<String> getStores(){
        ArrayList<String> stores = new ArrayList<String> ();
        stores.add("Gijon");
        stores.add("Madrid");
        stores.add("Burgos");
        stores.add("Oxford");
        stores.add("Nancy");
        return stores;
    }

    protected KafkaMessage consumMessage(){
        return this.kafka.getMessage(this.getTopicSubscription());
    }

    protected abstract void processMessage(KafkaMessage message);

    protected abstract void exit();


    // Getters
    protected KafkaBroker getKafka() {
        return kafka;
    }

    protected CBConfig getDb() {
        return db;
    }

    protected Cluster getCluster() {
        return cluster;
    }

    protected Bucket getBucket() {
        return bucket;
    }

    public boolean isFinish() {
        return finish;
    }

    public void setFinish(boolean finish) {
        this.finish = finish;
    }

    protected String getType(){
        return this.type;
    }

    protected String getTopicSubscription(){
        return this.topic_subscription;
    }

    protected UUID getInstance() {
        return this.instance;
    }

    protected String getID(){
        return type + "-" + instance.toString();
    }

}