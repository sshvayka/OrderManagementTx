package com.meccano.microservices;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBconfig;
import com.meccano.utils.MultiDocumentTransactionManager;
import com.meccano.utils.Pair;
import com.meccano.utils.Tuple3;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by ruben.casado.tejedor on 06/09/2016.
 */
public class SourcingOL extends MicroService {

    protected int locking_time;
    static Logger log = Logger.getLogger(SourcingOL.class.getName());


    public SourcingOL(KafkaBroker kafka, CBconfig db){
        super("SourcingOL",kafka,"Sourcing", db);
        this.locking_time= 30;
    }

    public SourcingOL(KafkaBroker kafka, CBconfig db, int time){
        super("SourcingOL",kafka,"Sourcing", db);
        this.locking_time=time;
    }

    protected String getRandomStore(StockVisibilityResponse stock, OrderFulfillmentResponse allocations, String item_id, int quantity){
        String store_id=null;
        ArrayList<Pair<String, Integer>> stocks= stock.stocks.get(item_id);
        boolean allocated=false;
        for (int i=0; i< stocks.size()&& !allocated;i++){
            String s_id= stocks.get(i).key;
            int store_stock= stocks.get(i).value.intValue();
            log.debug("Item to query: "+ item_id);
            log.debug("Store to query: "+ s_id);
            Hashtable<String, Integer> t= allocations.results.get(item_id);

            int store_allocations;
            if (t.containsKey(s_id))
                store_allocations = t.get(s_id);
            else
                store_allocations=0;

            if (store_stock -store_allocations>=quantity){

                log.debug("[ALLOCATED] Item: "+item_id+" Store: "+s_id+ " Store stock: "+store_stock+ " Store allocations: "+store_allocations+ " Requested: "+quantity);
                store_id= s_id;
                allocated=true;
            }

            else{
                log.debug("[REJECTED] Item: "+item_id+" Store: "+s_id+ " Store stock: "+store_stock+ " Store allocations: "+store_allocations+ " Requested: "+quantity);
                store_id=null;
            }

        }
        if (store_id== null)
            return null;
        return store_id+"-"+item_id;
    }

    protected void processMessage(KafkaMessage message) {
        SourcingRequest request = (SourcingRequest) message.getMessageBody();
        MultiDocumentTransactionManager tx = new MultiDocumentTransactionManager(this.db);
        ArrayList<JsonDocument> blocks =  new ArrayList<JsonDocument>();
        Iterator<String> itr = request.stocks.stock_id.iterator();

        //Transaction start
        tx.start();
        log.debug("Transaction start");
        //create order document
        JsonObject order = JsonObject.create()
                .put("_type", "Order")
                .put("order_id", request.order_id.toString())
                .put("state", "ALLOCATED");
        JsonArray suborders = JsonArray.create();
        int j=0;
        boolean success=true;
        while (itr.hasNext() && success) {
            String stock_id = itr.next();
            //get a random store with enough stock
            String id = this.getRandomStore(request.stocks, request.allocations, stock_id, (Integer) request.quantity.get(stock_id));
            //create locking_document
            if (id != null && tx.createLockDocument(id)) {
                log.debug("Lock document created: " + id+"_lock");

                // here we could use atomic operation to decrement phisic stock but we use the same
                // approach used in SourcingPL --> create the order document
                JsonDocument found = bucket.get(id);
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
                        .put("quantity", request.quantity.get(found.content().getString("item_id")));
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
                success=false;
            }

        }
        SourcingResponse body;

        //all documents has been successfully blocked so the transaction will commit
        if (success){
            order.put("suborders", suborders);
            JsonDocument doc = JsonDocument.create(request.order_id.toString(), order);
            JsonDocument inserted = bucket.upsert(doc);
            tx.commit();
            log.debug(request.order_id+ " - Order saved in CouchBase");
            //tx.close();
            log.debug("Transaction commited");
            body = new SourcingResponse(request.order_id, true);
        }
        else{
            body = new SourcingResponse(request.order_id, false);
        }


        //put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement", "SourcingResponse", body, this.getType(), message.getSource());
        this.kafka.putMessage("OrderManagement", msg);

    }



    protected void exit() {
        log.info("SourcingOL exit");

    }
}
