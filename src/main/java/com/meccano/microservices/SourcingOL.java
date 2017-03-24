package com.meccano.microservices;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBConfig;
import com.meccano.utils.MultiDocumentTransactionManager;
import com.meccano.utils.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.UUID;

public class SourcingOL extends MicroService {

//    private int locking_time;
    private static Logger log = LogManager.getLogger(SourcingOL.class);

    public SourcingOL(KafkaBroker kafka, CBConfig db){
        super("SourcingOL", kafka,"Sourcing", db);
//        this.locking_time = 3000;
    }

//    public SourcingOL(KafkaBroker kafka, CBConfig db, int time){
//        super("SourcingOL", kafka,"Sourcing", db);
//        this.locking_time = time;
//    }

    private String getRandomStore(StockVisibilityResponse stock, OrderFulfillmentResponse allocations, String itemId, int quantity){
//        ArrayList<Pair<String, Integer>> stocks = stock.getStocks().get(itemId);
//        String result = null;
//
//        for (Pair<String, Integer> pair : stocks){
//            String sId = pair.getKey();
//            int sStock = pair.getValue();
//            log.debug("Item to query: " + itemId);
//            log.debug("Store to query: " + sId);
//            Hashtable<String, Integer> t = allocations.getResults().get(itemId);
//
//            int sAllocations = 0;
//            if (t.containsKey(sId)) {
//                sAllocations = t.get(sId);
//            }
//            if (sStock - sAllocations >= quantity) {
//                log.debug("[ALLOCATED] Item: " + itemId + " Store: " + sId + " Store stock: " + sStock + " Store allocations: " + sAllocations + " Requested: " + quantity);
//                result = sId;
//                break;
//            } else {
//                log.debug("[REJECTED] Item: " + itemId + " Store: " + sId + " Store stock: " + sStock + " Store allocations: " + sAllocations + " Requested: " + quantity);
//            }
//        }
//        if(result != null){
//            result = sId + "-" + itemId;
//        }
//        return result;


        String storeId = null;
        ArrayList<Pair<String, Integer>> stocks = stock.getStocks().get(itemId);
        boolean allocated = false;
        for (int i = 0; i < stocks.size() && !allocated; i++){
            String sId = stocks.get(i).getKey();
            int storeStock = stocks.get(i).getValue();
            log.debug("Item to query: " + itemId);
            log.debug("Store to query: " + sId);
            Hashtable<String, Integer> t = allocations.getResults().get(itemId);

            int storeAllocations;
            if (t.containsKey(sId))
                storeAllocations = t.get(sId);
            else
                storeAllocations = 0;

            if (storeStock - storeAllocations >= quantity){
                log.debug("[ALLOCATED] Item: "+ itemId + " Store: " + sId + " Store stock: " + storeStock + " Store allocations: " + storeAllocations + " Requested: " + quantity);
                storeId = sId;
                allocated = true;
            } else{
                log.debug("[REJECTED] Item: " + itemId+" Store: " + sId + " Store stock: " + storeStock + " Store allocations: " + storeAllocations + " Requested: " + quantity);
                storeId = null;
            }
        }
        if (storeId == null)
            return null;
        return storeId + "-" + itemId;
    }

    protected void processMessage(KafkaMessage message) {
        SourcingRequest request = (SourcingRequest) message.getMessageBody();
        MultiDocumentTransactionManager tx = new MultiDocumentTransactionManager(super.getDb());
        Iterator<String> itr = request.getStocks().getStockId().iterator();

        //Transaction start
        tx.start();
        log.debug("Transaction start");
        //create order document
        JsonObject order = JsonObject.create()
                .put("_type", "Order")
                .put("orderId", request.getOrderId().toString())
                .put("state", "ALLOCATED");
        JsonArray suborders = JsonArray.create();
        boolean success = true;
        while (itr.hasNext() && success) {
            String stock_id = itr.next();
            //Get a random store with enough stock
            String id = this.getRandomStore(request.getStocks(), request.getAllocations(), stock_id, request.getQuantity().get(stock_id));
            // Create locking_document
            if (id != null && tx.createLockDocument(id)) { // TODO los archivos_lock se crean como null en couchbase, revisar
                log.debug("Lock document created: " + id + "_lock");

                // here we could use atomic operation to decrement phisic stock but we use the same
                // approach used in SourcingPL --> create the order document
                JsonDocument found = null;
                try {
                    found = super.getBucket().get(id);
                } catch (RuntimeException e){
                    log.error("Timeout exceeded at GET operation (" + e.getMessage() + ")");
                }
                //create suborder
                if(found != null) {
                    JsonObject suborder = JsonObject.create()
                            .put("suborderId", UUID.randomUUID().toString())
                            .put("storeId", found.content().getString("storeId"))
                            .put("state", "ALLOCATED");
                    //create item for each suborder
                    JsonArray items = JsonArray.create();
                    JsonObject item = JsonObject.create()
                            .put("itemId", found.content().getString("itemId"))
                            .put("price", found.content().getInt("price"))
                            .put("currency", found.content().getString("currency"))
                            .put("quantity", request.getQuantity().get(found.content().getString("itemId")));
                    items.add(item);
                    suborder.put("items", items);
                    suborders.add(suborder);
                    tx.partialCommit(id);
                } else{
                    log.error("Get de Couchbase devuelve null"); // TODO como actuar aqui??
                }
            } else { //not enough stock
                log.error("[ERROR] Not enough stock or already blocked document");
                log.debug("Transaction compensated");
                tx.rollback();
                //tx.close();
                success = false;
            }
        }
        SourcingResponse body;

        //all documents has been successfully blocked so the transaction will commit
        if (success){
            order.put("suborders", suborders);
            JsonDocument doc = JsonDocument.create(request.getOrderId().toString(), order);
            super.getBucket().upsert(doc);
            tx.commit();
            log.debug(request.getOrderId()+ " - Order saved in CouchBase");
            //tx.close();
            log.debug("Transaction commited");
            body = new SourcingResponse(request.getOrderId(), true);
        }
        else{
            body = new SourcingResponse(request.getOrderId(), false);
        }

        //put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement", "SourcingResponse", body, this.getType(), message.getSource());
        super.getKafka().putMessage("OrderManagement", msg);
    }

    protected void exit() {
        log.info("SourcingOL exit");
//        db.cluster.disconnect();
    }
}