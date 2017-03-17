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

    private int locking_time;
    private static Logger log = LogManager.getLogger(SourcingOL.class.getName());

    public SourcingOL(KafkaBroker kafka, CBConfig db){
        super("SourcingOL", kafka,"Sourcing", db);
        this.locking_time = 30;
    }

    public SourcingOL(KafkaBroker kafka, CBConfig db, int time){
        super("SourcingOL", kafka,"Sourcing", db);
        this.locking_time = time;
    }

    protected String getRandomStore(StockVisibilityResponse stock, OrderFulfillmentResponse allocations, String item_id, int quantity){
        String store_id = null;
        ArrayList<Pair<String, Integer>> stocks = stock.getStocks().get(item_id);
        boolean allocated = false;
        for (int i = 0; i< stocks.size() && !allocated; i++){
            String s_id = stocks.get(i).getKey();
            int store_stock = stocks.get(i).getValue();
            log.debug("Item to query: " + item_id);
            log.debug("Store to query: " + s_id);
            Hashtable<String, Integer> t = allocations.getResults().get(item_id);

            int store_allocations;
            if (t.containsKey(s_id))
                store_allocations = t.get(s_id);
            else
                store_allocations = 0;

            if (store_stock - store_allocations >= quantity){
                log.debug("[ALLOCATED] Item: "+item_id + " Store: " + s_id + " Store stock: " + store_stock + " Store allocations: " + store_allocations + " Requested: " + quantity);
                store_id = s_id;
                allocated = true;
            } else{
                log.debug("[REJECTED] Item: " + item_id+" Store: " + s_id + " Store stock: " + store_stock + " Store allocations: " + store_allocations + " Requested: " + quantity);
                store_id = null;
            }
        }
        if (store_id == null)
            return null;
        return store_id + "-" + item_id;
    }

    protected void processMessage(KafkaMessage message) {
        SourcingRequest request = (SourcingRequest) message.getMessageBody();
        MultiDocumentTransactionManager tx = new MultiDocumentTransactionManager(super.getDb());
        ArrayList<JsonDocument> blocks =  new ArrayList<JsonDocument>();
        Iterator<String> itr = request.getStocks().getStock_id().iterator();

        //Transaction start
        tx.start();
        log.debug("Transaction start");
        //create order document
        JsonObject order = JsonObject.create()
                .put("_type", "Order")
                .put("order_id", request.getOrder_id().toString())
                .put("state", "ALLOCATED");
        JsonArray suborders = JsonArray.create();
        int j = 0;
        boolean success = true;
        while (itr.hasNext() && success) {
            String stock_id = itr.next();
            //get a random store with enough stock
            String id = this.getRandomStore(request.getStocks(), request.getAllocations(), stock_id, (Integer) request.getQuantity().get(stock_id));
            //create locking_document
            if (id != null && tx.createLockDocument(id)) {
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
                JsonObject suborder = JsonObject.create()
                        .put("suborder_id", UUID.randomUUID().toString())
                        .put("store_id", found.content().getString("store_id"))
                        .put("state", "ALLOCATED");
                //create item for each suborder
                JsonArray items = JsonArray.create();
                JsonObject item = JsonObject.create()
                        .put("item_id", found.content().getString("item_id"))
                        .put("price", found.content().getInt("price"))
                        .put("currency", found.content().getString("currency"))
                        .put("quantity", request.getQuantity().get(found.content().getString("item_id")));
                items.add(item);
                suborder.put("items", items);
                suborders.add(suborder);
                tx.partialCommit(id);
                j++;
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
            JsonDocument doc = JsonDocument.create(request.getOrder_id().toString(), order);
            JsonDocument inserted = super.getBucket().upsert(doc);
            tx.commit();
            log.debug(request.getOrder_id()+ " - Order saved in CouchBase");
            //tx.close();
            log.debug("Transaction commited");
            body = new SourcingResponse(request.getOrder_id(), true);
        }
        else{
            body = new SourcingResponse(request.getOrder_id(), false);
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