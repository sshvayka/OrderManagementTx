package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;

import java.util.*;

/**
 * Created by ruben.casado.tejedor on 01/09/2016.
 */
public class OrderFulfillmentRequest implements MessageBody {

    private List<String> stores;
    private ArrayList<String> item_id;
    private UUID order_id;
    private StockVisibilityResponse stockVisibilityResponse;

    public OrderFulfillmentRequest(StockVisibilityResponse sr){
        this.order_id = sr.getOrder_id();
        this.item_id = sr.getStock_id();
        this.stockVisibilityResponse = sr;

        //get stores
        Collection<ArrayList<Pair<String, Integer>>> stocks = sr.getStocks().values();
        Set<String> set = new HashSet<String>();
        Iterator<ArrayList<Pair<String, Integer>>> itr = stocks.iterator();
        while (itr.hasNext()){
            ArrayList<Pair<String, Integer>> current  = itr.next();
            Iterator<Pair<String, Integer>> itr2= current.iterator();
            while (itr2.hasNext()){
                set.add(itr2.next().getKey());
            }
        }
        this.stores = new ArrayList<String>(set);
    }

    public List<String> getStores() {
        return stores;
    }

    public ArrayList<String> getItem_id() {
        return item_id;
    }

    public UUID getOrder_id() {
        return order_id;
    }

    public StockVisibilityResponse getStockVisibilityResponse() {
        return stockVisibilityResponse;
    }
}
