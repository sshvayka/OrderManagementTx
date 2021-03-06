package com.meccano.utils;

import com.meccano.Main;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class to check if all requests has been processed and them kill the threads
 */
public class ControlThread implements Runnable {

    private KafkaBroker kafka;

    // Number of instances of each Microservice (thread)
    private int nOrderManagement;
    private int nStockVisibility;
    private int nOrderFullfilment;
    private int nSourcing;

    private static Logger log = LogManager.getLogger(Main.class);

    public ControlThread(KafkaBroker kafka, int nOrderManagement, int nStockVisibility, int nOrderFullfilment, int nSourcing){
        this.kafka = kafka;
        this.nOrderManagement = nOrderManagement;
        this.nStockVisibility = nStockVisibility;
        this.nOrderFullfilment = nOrderFullfilment;
        this.nSourcing = nSourcing;
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
        for (int i = 0; i < this.nStockVisibility; i++) {
            KafkaMessage message = new KafkaMessage("StockVisibility", "Kill", null, null, null);
            this.kafka.putMessage("StockVisibility", message);
            log.info("StockVisibility kill message");
        }
        for (int i = 0; i < this.nOrderFullfilment; i++) {
            KafkaMessage message = new KafkaMessage("OrderFulfillment", "Kill", null, null, null);
            this.kafka.putMessage("OrderFulfillment", message);
            log.info("OrderFulfillment kill message");
        }
        for (int i = 0; i < this.nSourcing; i++) {
            KafkaMessage message = new KafkaMessage("Sourcing", "Kill", null, null, null);
            this.kafka.putMessage("Sourcing", message);
            log.info("Sourcing kill message");
        }
        for (int i = 0; i < this.nOrderManagement; i++) {
            KafkaMessage message = new KafkaMessage("OrderManagement", "Kill", null, null, null);
            this.kafka.putMessage("OrderManagement", message);
            log.info("OrderManagement kill message");
        }
        log.info("Control exit");
    }
}