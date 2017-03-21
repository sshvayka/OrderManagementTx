package com.meccano.microservices;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.view.ViewRow;
import com.meccano.kafka.MessageBody;

import java.util.*;

public class OrderFulfillmentResponse implements MessageBody {

    private Hashtable<String, Hashtable<String,Integer>> results; // <item_id, <store_id-item_id, quantity>>
    private UUID orderId;
    private StockVisibilityResponse stockVisibilityResponse;


    public OrderFulfillmentResponse(UUID orderId, Hashtable<String,List<ViewRow>> result, StockVisibilityResponse sr){
        this.orderId = orderId;
        this.stockVisibilityResponse = sr;
        results = new Hashtable<>();
        for (String key : result.keySet()){
            Hashtable<String, Integer> temp = new Hashtable<>();
            for(ViewRow row : result.get(key)){
                JsonArray j = (JsonArray) row.key();
                temp.put(j.getString(1), (Integer) row.value());
            }
            results.put(key, temp);
        }
    }

    public Hashtable<String, Hashtable<String, Integer>> getResults() {
        return results;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public StockVisibilityResponse getStockVisibilityResponse() {
        return stockVisibilityResponse;
    }
}