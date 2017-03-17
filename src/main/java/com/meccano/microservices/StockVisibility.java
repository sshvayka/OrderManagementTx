package com.meccano.microservices;

import com.couchbase.client.java.document.JsonDocument;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBConfig;
import com.meccano.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StockVisibility extends MicroService {

    private static Logger log = LogManager.getLogger(StockVisibility.class);

    public StockVisibility(KafkaBroker kafka, CBConfig db){
        super("StockVisibility", kafka, "StockVisibility", db);
        log.info("StockVisibility MS thread created");
    }

    @Override
    protected void processMessage(KafkaMessage message) {
        // Get message from Kafka
        // There is one topic for each possible destination MS
        StockVisibilityRequest request= (StockVisibilityRequest)message.getMessageBody();

        ArrayList<String> stores = super.getStores();
        StockVisibilityResponse  stock = new StockVisibilityResponse(request.getOrder_id(), request.getStock_id(), request.getOrderManagementRequest().getItems());
        Iterator<String> itr = request.getStock_id().iterator();
        log.debug(request.getOrder_id() + " - Number of items for stock request: " + request.getStock_id().size());
        // Iterate all the products and for each product, all the associate store
        while (itr.hasNext()){
            String item_id = itr.next();
            for (String store_id : stores) {
                // The document_id is store_id-item_id
                String id = store_id + "-" + item_id;
                log.debug(id);
                JsonDocument found = null;
                try {
                    found = super.getBucket().get(id);
                } catch (RuntimeException e){
                    log.error("Timeout exceeded at GET operation (" + e.getMessage() + ")");
                }
                if (found != null){
                    Integer quantity = found.content().getInt("quantity") - 1; // 1 item is 0 to use atomic opr
                    stock.add(item_id, new Pair<String, Integer>(store_id, quantity));

                } else {
                    stock.add(item_id, new Pair<String, Integer>(store_id, 0));
                    log.debug(request.getOrder_id() + " - Item not found in Couchbase: " + id);
                }
            }
        }
        // Put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","StockVisibilityResponse", stock, this.getType(), message.getSource());
        super.getKafka().putMessage("OrderManagement", msg);
    }

    @Override
    protected void exit() {
        log.info("StockVisibility exit");
//        db.cluster.disconnect();
    }
}
