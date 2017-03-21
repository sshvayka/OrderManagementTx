package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.UUID;

public class StockVisibilityResponse implements MessageBody {

    private UUID orderId;
    private ArrayList<Pair<String, Integer>> quantity;
    private ArrayList<String> stockId;
    private Hashtable<String, ArrayList<Pair<String,Integer>>> stocks;

    private static Logger log = LogManager.getLogger(StockVisibilityResponse.class);

    public StockVisibilityResponse (UUID orderId, ArrayList<String> stockId, ArrayList<Pair<String, Integer>> quantity){
        this.stockId = stockId;
        this.orderId = orderId;
        this.stocks = new Hashtable<>();
        for(String item : stockId){
            this.stocks.put(item, new ArrayList<>());
        }
        // Items and quantity required
        this.quantity = quantity;
    }

    protected void add(String itemId, Pair<String,Integer> p){
        ArrayList<Pair<String,Integer>> temp = this.stocks.get(itemId);
        if (temp != null){
            temp.add(p);
            stocks.put(itemId, temp);
        } else {
            log.error("Error in ADD method - there is no item : " + itemId);
        }
    }

    public UUID getOrderId() {
        return orderId;
    }

    public ArrayList<Pair<String, Integer>> getQuantity() {
        return quantity;
    }

    public ArrayList<String> getStockId() {
        return stockId;
    }

    public Hashtable<String, ArrayList<Pair<String, Integer>>> getStocks() {
        return stocks;
    }
}
