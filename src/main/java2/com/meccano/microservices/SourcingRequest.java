package com.meccano.microservices;

import com.meccano.utils.Pair;
import com.meccano.utils.Tuple3;

import java.util.*;

/**
 * Created by ruben.casado.tejedor on 06/09/2016.
 */
public class SourcingRequest implements com.meccano.kafka.MessageBody{

    public StockVisibilityResponse stocks;
    public OrderFulfillmentResponse allocations;
    public Hashtable<String, Integer> quantity;
    public UUID order_id;

    public SourcingRequest(UUID order_id, StockVisibilityResponse stocks, OrderFulfillmentResponse allocations, ArrayList<Pair<String, Integer>> quantity){
        this.order_id=order_id;
        this.stocks=stocks;
        this.allocations=allocations;
        this.quantity= new Hashtable<String, Integer>();
        Iterator<Pair<String, Integer>> itr= quantity.iterator();
        while (itr.hasNext()){
            Pair<String, Integer> item= itr.next();
            this.quantity.put(item.key, item.value);
        }
    }


}
