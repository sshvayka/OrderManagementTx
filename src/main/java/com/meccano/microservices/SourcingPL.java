package com.meccano.microservices;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBConfig;
import com.meccano.utils.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class SourcingPL extends MicroService {

    private int lockingTime;
    private static Logger log = LogManager.getLogger(SourcingPL.class);

    public SourcingPL(KafkaBroker kafka, CBConfig db){
        super("SourcingPL", kafka,"Sourcing", db);
        this.lockingTime = 30;
    }

    public SourcingPL(KafkaBroker kafka, CBConfig db, int time){
        super("SourcingPL", kafka,"SourcingRequest", db);
        this.lockingTime = time;
    }

    private String getRandomStore(StockVisibilityResponse stock, OrderFulfillmentResponse allocations, String itemId, int quantity){
        String storeId = null;
        ArrayList<Pair<String, Integer>> stocks = stock.getStocks().get(itemId);
        boolean allocated = false;
        for (int i = 0; i< stocks.size() && !allocated; i++){
            String s_id = stocks.get(i).getKey();
            int storeStock = stocks.get(i).getValue();
            log.debug("Item to query: " + itemId);
            log.debug("Store to query: " + s_id);
            Hashtable<String,Integer> t = allocations.getResults().get(itemId);

            int storeAllocations;
            if (t.containsKey(s_id)){
                storeAllocations = t.get(s_id);
            } else {
                storeAllocations = 0;
            }

            if (storeStock - storeAllocations >= quantity){
                log.debug("[ALLOCATED] Item: " + itemId + " Store: "+s_id+ " Store stock: " + storeStock + " Allocations store: " + storeAllocations + " Requested: " + quantity);
                storeId = s_id;
                allocated = true;
            } else {
                log.debug("[REJECTED] Item: " + itemId + " Store: "+s_id+ " Store stock: " + storeStock +  " Allocations store: " + storeAllocations + " Requested: " + quantity);
                storeId = null;
            }
        }
        if (storeId == null)
            return null;
        return storeId + "-" + itemId;
    }

    protected void processMessage(KafkaMessage message) {
        SourcingRequest request = (SourcingRequest)message.getMessageBody();
        ArrayList<JsonDocument> blocks = new ArrayList<>();

        //iterate all item_id
        for (String stockId : request.getStocks().getStockId()) {
            //get a random store with enough stock
            String id = this.getRandomStore(request.getStocks(), request.getAllocations(), stockId, request.getQuantity().get(stockId));
            //block the document, if it is already blocked the transaction is aborted
            try {
                log.debug(request.getOrderId() + "Document to lock: " + id);
                if (id != null) {
                    JsonDocument found = super.getBucket().getAndLock(id, this.lockingTime);
                    blocks.add(found);
                } else {
                    log.error("[ERROR] SourcingPL not enough elements");
                    this.abort(request.getOrderId(), blocks);
                    return;
                }
            } catch (Exception e) {
                log.error("[ERROR] SourcingPL blocking element");
                this.abort(request.getOrderId(), blocks);
                return;
            }
        }
        // All documents has been successfully blocked so the transaction will commit
        this.createOrder(request.getOrderId().toString(), blocks, request.getQuantity());
        this.unlockDocuments(blocks);
        SourcingResponse body = new SourcingResponse(request.getOrderId(),true);
        // Put in Kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","SourcingResponse", body, this.getType(), message.getSource());
        super.getKafka().putMessage("OrderManagement", msg);
    }

    // Cancel an on-going transaction
    private void abort(UUID orderId, ArrayList<JsonDocument> blocks){
        this.unlockDocuments(blocks);
        SourcingResponse body = new SourcingResponse(orderId,false);
        // Put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","SourcingResponse", body, this.getType(), "OrderManagement");
        super.getKafka().putMessage("OrderManagement", msg);
    }

    private void unlockDocuments(ArrayList<JsonDocument> blocks){
        for (JsonDocument block : blocks) {
            String documentId = block.id();
//            long cas = block.cas();
            super.getBucket().unlock(block);
        }
    }

    // Create the Order document
    private void createOrder(String orderId, ArrayList<JsonDocument> blocks, Hashtable<String,Integer> quantity){
        // Create order document
        JsonObject order = JsonObject.create()
                .put("_type", "Order")
                .put("orderId", orderId)
                .put("state", "ALLOCATED");

        // Create suborders - one for each item (could be improved to group items from the same store)
        JsonArray suborders = JsonArray.create();
        for (JsonDocument block : blocks) {
            JsonObject suborder = JsonObject.create()
                    .put("suborderId", UUID.randomUUID().toString())
                    .put("storeId", block.content().getString("storeId"))
                    .put("state", "ALLOCATED");
            // Create item for each suborder
            JsonArray items = JsonArray.create();
            JsonObject item = JsonObject.create()
                    .put("itemId", block.content().getString("itemId"))
                    .put("price", block.content().getInt("price"))
                    .put("currency", block.content().getString("currency"))
                    .put("quantity", quantity.get(block.content().getString("itemId")));
            items.add(item);
            suborder.put("items", items);
            suborders.add(suborder);
        }
        order.put("suborders", suborders);
        JsonDocument doc = JsonDocument.create(orderId, order);
        super.getBucket().upsert(doc);
        log.debug(orderId + " - Order saved in CouchBase");
    }

    protected void exit() {
        log.info("SourcingPL exit");
    }
}