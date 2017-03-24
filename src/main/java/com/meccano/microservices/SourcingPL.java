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

    private int locking_time;
    private static Logger log = LogManager.getLogger(SourcingPL.class);

    public SourcingPL(KafkaBroker kafka, CBConfig db){
        super("SourcingPL", kafka,"Sourcing", db);
        this.locking_time = 30;
    }

    public SourcingPL(KafkaBroker kafka, CBConfig db, int time){
        super("SourcingPL", kafka,"SourcingRequest", db);
        this.locking_time = time;
    }

    private String getRandomStore(StockVisibilityResponse stock, OrderFulfillmentResponse allocations, String item_id, int quantity){
        String store_id = null;
        ArrayList<Pair<String, Integer>> stocks = stock.getStocks().get(item_id);
        boolean allocated = false;
        for (int i = 0; i< stocks.size() && !allocated; i++){
            String s_id = stocks.get(i).getKey();
            int store_stock = stocks.get(i).getValue();
            log.debug("Item to query: " + item_id);
            log.debug("Store to query: " + s_id);
            Hashtable<String,Integer> t = allocations.getResults().get(item_id);

            int store_allocations;
            if (t.containsKey(s_id)){
                store_allocations = t.get(s_id);
            } else {
                store_allocations = 0;
            }

            if (store_stock - store_allocations >= quantity){
                log.debug("[ALLOCATED] Item: "+item_id+" Store: "+s_id+ " Store stock: "+store_stock+ " Allocations store: "+store_allocations+ " Requested: "+quantity);
                store_id = s_id;
                allocated = true;
            } else {
                log.debug("[REJECTED] Item: "+item_id+" Store: "+s_id+ " Store stock: "+store_stock+ " Allocations store: "+store_allocations+ " Requested: "+quantity);
                store_id = null;
            }
        }
        if (store_id == null)
            return null;
        return store_id + "-" + item_id;
    }

    protected void processMessage(KafkaMessage message) {
        SourcingRequest request = (SourcingRequest)message.getMessageBody();
        ArrayList<JsonDocument> blocks = new ArrayList<>();

        //iterate all item_id
        for (String stock_id : request.getStocks().getStockId()) {
            //get a random store with enough stock
            String id = this.getRandomStore(request.getStocks(), request.getAllocations(), stock_id, request.getQuantity().get(stock_id));
            //block the document, if it is already blocked the transaction is aborted
            try {
                log.debug(request.getOrderId() + "Document to lock: " + id);
                if (id != null) {
                    JsonDocument found = super.getBucket().getAndLock(id, this.locking_time);
                    blocks.add(found);
                } else {
                    log.error("[ERROR] SourcingPL not enough elements");
                    this.abort(request.getOrderId(), blocks);
                    return;
                }
            } catch (Exception e) {
                log.error("[ERROR] SourcingPL blocking element " + e.toString());
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
    private void abort(UUID order_id, ArrayList<JsonDocument> blocks){
        this.unlockDocuments(blocks);
        SourcingResponse body = new SourcingResponse(order_id,false);
        // Put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","SourcingResponse", body, this.getType(), "OrderManagement");
        super.getKafka().putMessage("OrderManagement", msg);
    }

    private void unlockDocuments(ArrayList<JsonDocument> blocks){
        for (JsonDocument block : blocks) {
            String document_id = block.id();
            long cas = block.cas();
            super.getBucket().unlock(document_id, cas);// TODO lanza TimeOut Exception, manejar este error
        }
    }

    // Create the Order document
    private void createOrder(String order_id, ArrayList<JsonDocument> blocks, Hashtable<String,Integer> quantity){
        // Create order document
        JsonObject order = JsonObject.create()
                .put("_type", "Order")
                .put("orderId", order_id)
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
        JsonDocument doc = JsonDocument.create(order_id, order);
        super.getBucket().upsert(doc);
        log.debug(order_id + " - Order saved in CouchBase");
    }

    protected void exit() {
        log.info("SourcingPL exit");
    }
}