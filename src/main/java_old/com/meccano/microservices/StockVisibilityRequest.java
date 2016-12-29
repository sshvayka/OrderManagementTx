package com.meccano.microservices;

import com.meccano.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 */
public class StockVisibilityRequest implements com.meccano.kafka.MessageBody{


    public StockVisibilityRequest(OrderManagementRequest request){
        this.order_id=request.order_id;
        this.stock_id= new ArrayList<String>();
        this.orderManagementRequest=request;

        Iterator<Pair<String, Integer>> itr = request.items.iterator();
        while (itr.hasNext()){
            stock_id.add(itr.next().key);
        }

    }

    public ArrayList<String> stock_id; //array of item_is
    public UUID order_id;
    public OrderManagementRequest orderManagementRequest;
}
