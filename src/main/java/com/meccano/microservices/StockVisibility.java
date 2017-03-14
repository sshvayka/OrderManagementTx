package com.meccano.microservices;

import com.couchbase.client.java.document.JsonDocument;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBconfig;
import com.meccano.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 */
public class StockVisibility extends MicroService {

    private static Logger log = LogManager.getLogger(StockVisibility.class);

    public StockVisibility(KafkaBroker kafka, CBconfig db){
        super("StockVisibility", kafka, "StockVisibility", db);
        log.info("StockVisibility MS thread created");
    }

    @Override
    protected void processMessage(KafkaMessage message) {
        // Get message from Kafka
        // There is one topic for each possible destination MS
        StockVisibilityRequest request= (StockVisibilityRequest)message.getMessageBody();

        ArrayList<String> stores = getStores();
        StockVisibilityResponse  stock = new StockVisibilityResponse(request.order_id, request.stock_id, request.orderManagementRequest.items);
        Iterator<String> itr = request.stock_id.iterator();
        log.debug(request.order_id + " - Number of items for stock request: " + request.stock_id.size());
        // Iterate all the products and for each product, all the associate store
        while (itr.hasNext()){
            String item_id = itr.next();
            for (String store_id : stores) {
                // The document_id is store_id-item_id
                String id = store_id + "-" + item_id;
                log.debug(id);
                JsonDocument found = null;
                try {
                    found = bucket.get(id);
                } catch (RuntimeException e){
                    log.error("Timeout exceeded at GET operation (" + e.getMessage() + ")");
                }
                if (found != null){
                    Integer quantity = found.content().getInt("quantity") - 1; // 1 item is 0 to use atomic opr
                    stock.add(item_id, new Pair<String, Integer>(store_id, quantity));

                } else {
                    stock.add(item_id, new Pair<String, Integer>(store_id, 0));
                    log.debug(request.order_id + " - Item not found in Couchbase: " + id);
                }
            }
        }
        // Put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","StockVisibilityResponse", stock, this.getType(), message.getSource());
        this.kafka.putMessage("OrderManagement", msg);
    }

    @Override
    protected void exit() {
        log.info("StockVisibility exit");
//        db.cluster.disconnect();
    }

    // Define the set of stores associated to this MS instance
    protected ArrayList<String> getStores(){
        ArrayList<String> stores = new ArrayList<String> ();
        //mock
        stores.add("Gijon");
        stores.add("Madrid");
        stores.add("Burgos");
        stores.add("Oxford");
        stores.add("Nancy");
        return stores;
    }
}
