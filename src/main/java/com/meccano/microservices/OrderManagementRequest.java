package com.meccano.microservices;

import com.meccano.kafka.MessageBody;
import com.meccano.utils.Pair;

import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 20/09/2016.
 */
public class OrderManagementRequest implements MessageBody {

    public ArrayList<Pair<String,Integer>> items; // Array of item_id and quantity
    public UUID order_id;
    public long order_start;

    public OrderManagementRequest(UUID order_id, ArrayList<Pair<String, Integer>> stock_id) {
        this.items = stock_id;
        this.order_id = order_id;
    }
}
