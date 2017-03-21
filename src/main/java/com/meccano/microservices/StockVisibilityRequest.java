package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

public class StockVisibilityRequest implements MessageBody {

    private ArrayList<String> stockId; // Array of item_is
    private UUID orderId;
    private OrderManagementRequest orderManagementRequest;

    public StockVisibilityRequest(OrderManagementRequest request){
        this.orderId = request.getOrderId();
        this.stockId = new ArrayList<>();
        this.orderManagementRequest = request;
        for(Pair<String, Integer> item : request.getItems()){
            stockId.add(item.getKey());
        }
    }

    public ArrayList<String> getStockId() {
        return stockId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public OrderManagementRequest getOrderManagementRequest() {
        return orderManagementRequest;
    }
}
