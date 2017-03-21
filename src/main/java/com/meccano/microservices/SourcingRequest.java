package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;

import java.util.*;

public class SourcingRequest implements MessageBody {

    private StockVisibilityResponse stocks;
    private OrderFulfillmentResponse allocations;
    private Hashtable<String, Integer> quantity;
    private UUID orderId;

    public SourcingRequest(UUID orderId, StockVisibilityResponse stocks, OrderFulfillmentResponse allocations, ArrayList<Pair<String, Integer>> quantity){
        this.orderId = orderId;
        this.stocks = stocks;
        this.allocations = allocations;
        this.quantity = new Hashtable<>();
        for(Pair<String, Integer> item : quantity){
            this.quantity.put(item.getKey(), item.getValue());
        }
    }

    public StockVisibilityResponse getStocks() {
        return stocks;
    }

    public OrderFulfillmentResponse getAllocations() {
        return allocations;
    }

    public Hashtable<String, Integer> getQuantity() {
        return quantity;
    }

    public UUID getOrderId() {
        return orderId;
    }
}
