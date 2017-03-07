package com.meccano.utils;

import com.meccano.Main;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.microservices.OrderManagementRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 20/09/2016.
 */
public class RequestGenerator implements Runnable {

    protected KafkaBroker kafka;
    protected int n_orders;
    protected int frequency;
    protected int variety;
    static Logger log = LogManager.getLogger(Main.class.getName());

    public RequestGenerator (KafkaBroker kafka, int num, int fre, int var){
        this.kafka = kafka;
        this.n_orders = num;
        this.frequency = fre;
        this.variety = var;
    }

    public void run(){
        // Create the orders request
        for (int i = 0; i < this.n_orders; i++){
            // Generate a new order_id
            UUID order_id = UUID.randomUUID();
            // Generate the order items and requested quantities [always 3 items per order]
            ArrayList<Pair<String, Integer>> items = this.getRandomItemsAndQuantities(3, this.variety);
            // Create the OrderManagemenentRequest request and send message to Kafka
            OrderManagementRequest or = new OrderManagementRequest(order_id, items);
            KafkaMessage message = new KafkaMessage("OrderManagement","OrderManagementRequest", or, "RequestGenerator","OrderManagement");
            this.kafka.putMessage("OrderManagement", message);
            try {
                Thread.sleep(frequency);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected ArrayList<Pair<String, Integer>> getRandomItemsAndQuantities(int number, int variety){
        Random rnd= new Random(System.currentTimeMillis());
        ArrayList<Pair<String, Integer>> items = new ArrayList<Pair<String, Integer>>();
        for (int i = 0; i < number; i++){
            Integer r = rnd.nextInt(variety) + 1;
            Integer v = rnd.nextInt(3) + 1;
            Pair element = new Pair(r.toString(), v);
            if (!items.contains(element))
                items.add(element);
        }
        return items;
    }
}