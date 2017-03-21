package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;

import java.util.ArrayList;
import java.util.UUID;

public class OrderManagementRequest implements MessageBody {

    private ArrayList<Pair<String,Integer>> items; // Array of item_id and quantity
    private UUID orderId;
//    private long orderStart;

    public OrderManagementRequest(UUID orderId, ArrayList<Pair<String, Integer>> stockId) {
        this.items = stockId;
        this.orderId = orderId;
    }

    public ArrayList<Pair<String, Integer>> getItems() {
        return items;
    }

    public UUID getOrderId() {
        return orderId;
    }

//    public long getOrderStart() {
//        return orderStart;
//    }
}
