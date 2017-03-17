package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;

import java.util.*;

/**
 * Created by ruben.casado.tejedor on 06/09/2016.
 */
public class SourcingRequest implements MessageBody {

    private StockVisibilityResponse stocks;
    private OrderFulfillmentResponse allocations;
    private Hashtable<String, Integer> quantity;
    private UUID order_id;

    public SourcingRequest(UUID order_id, StockVisibilityResponse stocks, OrderFulfillmentResponse allocations, ArrayList<Pair<String, Integer>> quantity){
        this.order_id = order_id;
        this.stocks = stocks;
        this.allocations = allocations;
        this.quantity = new Hashtable<String, Integer>();
        Iterator<Pair<String, Integer>> itr = quantity.iterator();
        while (itr.hasNext()){
            Pair<String, Integer> item = itr.next();
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

    public UUID getOrder_id() {
        return order_id;
    }
}
