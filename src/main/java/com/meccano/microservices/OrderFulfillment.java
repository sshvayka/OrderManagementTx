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
import java.util.List;

public class OrderFulfillment extends MicroService {

    private static Logger log = LogManager.getLogger(OrderFulfillment.class);

    public OrderFulfillment (KafkaBroker kafka, CBConfig db){
        super("OrderFulfillment", kafka, "OrderFulfillment", db);
    }

    protected void processMessage(KafkaMessage message) {
        OrderFulfillmentRequest request = (OrderFulfillmentRequest)message.getMessageBody();
        Hashtable<String,List<ViewRow>> results = new Hashtable<>();
        JsonArray keys = JsonArray.create();
        // Iterate all items
        for(String itemId : request.getItemId()){
            for (String storeId : request.getStores()){
                //["item_id","store_id"]
                JsonArray j = JsonArray.create();
                j.add(itemId);
                j.add(storeId);
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
            results.put(itemId, rowResult);
        }

        OrderFulfillmentResponse body = new OrderFulfillmentResponse(request.getOrderId(), results, request.getStockVisibilityResponse());
        // Put in Kafka the response message
        KafkaMessage msg = new KafkaMessage("OrderManagement","OrderFulfillmentResponse", body, this.getType(), message.getSource());
        super.getKafka().putMessage("OrderManagement", msg);
        log.debug(request.getOrderId() + " - Created Kafka message in topic OrderManagement. Type: OrderFulfillmentResponse");
    }

    protected void exit() {
        log.info("OrderFulfillment exit");
//        db.cluster.disconnect();
    }
}
