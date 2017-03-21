package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;

import java.util.*;

public class OrderFulfillmentRequest implements MessageBody {

    private List<String> stores;
    private ArrayList<String> itemId;
    private UUID orderId;
    private StockVisibilityResponse stockVisibilityResponse;

    public OrderFulfillmentRequest(StockVisibilityResponse sr){
        this.orderId = sr.getOrderId();
        this.itemId = sr.getStockId();
        this.stockVisibilityResponse = sr;

        //get stores
        Collection<ArrayList<Pair<String, Integer>>> stocks = sr.getStocks().values();
        Set<String> set = new HashSet<>();
        for (ArrayList<Pair<String, Integer>> current : stocks) {
            for (Pair<String, Integer> aCurrent : current) {
                set.add(aCurrent.getKey());
            }
        }
        this.stores = new ArrayList<>(set);
    }

    public List<String> getStores() {
        return stores;
    }

    public ArrayList<String> getItemId() {
        return itemId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public StockVisibilityResponse getStockVisibilityResponse() {
        return stockVisibilityResponse;
    }
}
