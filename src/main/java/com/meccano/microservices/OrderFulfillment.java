package com.meccano.microservices;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

public class OrderFulfillment extends MicroService {

    private static Logger log = LogManager.getLogger(OrderFulfillment.class);

    public OrderFulfillment (KafkaBroker kafka, CBConfig db){
        super("OrderFulfillment", kafka, "OrderFulfillment", db);
    }

    protected void processMessage(KafkaMessage message) {
        OrderFulfillmentRequest request= (OrderFulfillmentRequest)message.getMessageBody();

        JsonArray keys = JsonArray.create();
        // iterate all items
        Iterator<String> itr = request.getItem_id().iterator();
        Hashtable<String,List<ViewRow>> results = new Hashtable<String, List<ViewRow>>();
        while (itr.hasNext()){
            //for each item, get the allocation of each associate store
            String item_id = itr.next();
            for (String s: request.getStores()){
                //["item_id","store_id"]
                JsonArray j = JsonArray.create();
                j.add(item_id);
                j.add(s);
                keys.add(j);
            }
            List<ViewRow> rowResult = new ArrayList<>();
            try {
                ViewQuery query = ViewQuery.from("OrderFulfillment", "allocations").group().reduce().keys(keys);
                ViewResult result = super.getBucket().query(query);
                rowResult = result.allRows();
            } catch (RuntimeException e){
                log.error("Error del Observable al hacer la operacion con la Vista (" + e.getMessage() + ")");
            }
//            log.debug(request.order_id + " Resultados MapReduce:" + rowResult.size());
            results.put(item_id, rowResult);
        }

        OrderFulfillmentResponse body = new OrderFulfillmentResponse(request.getOrder_id(), results, request.getStockVisibilityResponse());
        //put in kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","OrderFulfillmentResponse", body, this.getType(), message.getSource());
        super.getKafka().putMessage("OrderManagement", msg);
        log.debug(request.getOrder_id() + " - Created Kafka message in topic OrderManagement. Type: OrderFulfillmentResponse");
    }

    protected void exit() {
        log.info("OrderFulfillment exit");
//        db.cluster.disconnect();
    }
}
