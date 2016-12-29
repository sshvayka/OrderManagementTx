package com.meccano.microservices;

import com.meccano.utils.Pair;

import java.util.*;

/**
 * Created by ruben.casado.tejedor on 01/09/2016.
 */
public class OrderFulfillmentRequest implements com.meccano.kafka.MessageBody{

    public List<String> stores;
    public ArrayList<String> item_id;
    public UUID order_id;
    public StockVisibilityResponse stockVisibilityResponse;

    public OrderFulfillmentRequest(StockVisibilityResponse sr){
        this.order_id=sr.order_id;
        this.item_id=sr.stock_id;
        this.stockVisibilityResponse=sr;
        //get stores
        Collection<ArrayList<Pair<String, Integer>>> stocks= sr.stocks.values();
        Set<String> set = new HashSet<String>();
        Iterator<ArrayList<Pair<String, Integer>>> itr = stocks.iterator();
        while (itr.hasNext()){
            ArrayList<Pair<String, Integer>> current  = itr.next();
            Iterator<Pair<String, Integer>> itr2= current.iterator();
            while (itr2.hasNext()){
                set.add(itr2.next().key);
            }
        }
        this.stores=new ArrayList<String>(set);
    }

    //antiguo, se puede borrar
    public OrderFulfillmentRequest(UUID order_id, ArrayList<String> item_id, List<String> stores){
        this.stores=stores;
        this.item_id=item_id;
        this.order_id=order_id;
    }
}
