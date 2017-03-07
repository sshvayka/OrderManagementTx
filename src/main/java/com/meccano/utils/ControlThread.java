package com.meccano.utils;

import com.meccano.Main;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by ruben.casado.tejedor on 29/12/2016.
 *
 * Class to check if all requests has been processed and them kill the threads
 */
public class ControlThread implements Runnable {

    protected KafkaBroker kafka;

    // Number of instances of each microservice (thread)
    protected int n_om;
    protected int n_sv;
    protected int n_of;
    protected int n_so;

    static Logger log = LogManager.getLogger(Main.class.getName());

    public ControlThread(KafkaBroker kafka, int n_om, int n_sv, int of, int so){
        this.kafka = kafka;
        this.n_om = n_om;
        this.n_sv = n_sv;
        this.n_of = of;
        this.n_so = so;
    }

    public void run() {
        // Check number of pending requests (messages in Kafka)
        log.info("ControlThread - Nº Kafka Sms: " + this.kafka.totalSize());

        //wait until all kafka messages has been procceded
        int pending = this.kafka.totalSize();
        while (pending != 0){
            pending = this.kafka.totalSize();
        }

        // Wait until live threads process current sms
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Send kafka message to all microservices to kill them
        for (int i = 0; i < this.n_sv; i++) {
            KafkaMessage message = new KafkaMessage("StockVisibility", "Kill", null, null, null);
            this.kafka.putMessage("StockVisibility", message);
            log.info("StockVisibility kill message");
        }
        for (int i = 0; i < this.n_of; i++) {
            KafkaMessage message = new KafkaMessage("OrderFulfillment", "Kill", null, null, null);
            this.kafka.putMessage("OrderFulfillment", message);
            log.info("OrderFulfillment kill message");

        }
        for (int i = 0; i < this.n_so; i++) {
            KafkaMessage message = new KafkaMessage("Sourcing", "Kill", null, null, null);
            this.kafka.putMessage("Sourcing", message);
            log.info("Sourcing kill message");

        }
        for (int i = 0; i < this.n_om; i++) {
            KafkaMessage message = new KafkaMessage("OrderManagement", "Kill", null, null, null);
            this.kafka.putMessage("OrderManagement", message);
            log.info("OrderManagement kill message");
        }
        log.info("Control exit");
    }
}