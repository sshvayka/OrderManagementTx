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
        StockVisibilityRequest request= (StockVisibilityRequest) message.getMessageBody();

        ArrayList<String> stores = super.getStores();
        StockVisibilityResponse  stock = new StockVisibilityResponse(request.getOrderId(), request.getStockId(), request.getOrderManagementRequest().getItems());
//        Iterator<String> itr = request.getStock_id().iterator();
        log.debug(request.getOrderId() + " - Number of items for stock request: " + request.getStockId().size());
        // Iterate all the products and for each product, all the associate store
        for(String itemId : request.getStockId()){
            for(String storeId : stores){
                // The document_id is store_id-item_id
                String id = storeId + "-" + itemId;
                log.debug(id);
                JsonDocument found = null;
                try {
                    found = super.getBucket().get(id);
                } catch (RuntimeException e){
                    log.error("Timeout exceeded at GET operation (" + e.getMessage() + ")");
                }
                if (found != null){
                    Integer quantity = found.content().getInt("quantity") - 1; // 1 item is 0 to use atomic opr
                    stock.add(itemId, new Pair<>(storeId, quantity));
                } else {
                    stock.add(itemId, new Pair<>(storeId, 0));
                    log.debug(request.getOrderId() + " - Item not found in Couchbase: " + id);
                }
            }
        }
        // Put in Kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","StockVisibilityResponse", stock, this.getType(), message.getSource());
        super.getKafka().putMessage("OrderManagement", msg);
    }

    @Override
    protected void exit() {
        log.info("StockVisibility exit");
//        db.cluster.disconnect();
    }
}
