package com.meccano.microservices;

import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBconfig;
import com.meccano.utils.Pair;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ruben.casado.tejedor on 08/09/2016.
 */
public class OrderManagement extends MicroService {

    protected BufferedWriter file;
    protected String path;
    static Logger log = Logger.getLogger(OrderManagement.class.getName());

    public OrderManagement(KafkaBroker kafka, CBconfig db, String path) throws IOException {
        super ("OrderManagement", kafka, "OrderManagement", db);
        this.path=path;

    }

    protected void processOrderRequest(OrderManagementRequest request){
        //create the StockVisibility request and send message to Kafka
        StockVisibilityRequest sr = new StockVisibilityRequest(request);
        KafkaMessage message = new KafkaMessage("StockVisibility","StockVisibilityRequest",sr, this.getType(),"StockVisibility");
        this.kafka.putMessage("StockVisibility", message);

    }

    protected void processStockVisibilityResponse (StockVisibilityResponse response){
        // TO DO
        OrderFulfillmentRequest of = new OrderFulfillmentRequest(response);
        KafkaMessage message = new KafkaMessage("OrderFulfillment","OrderFulfillmentRequest",of, this.getType(),"OrderFulfillment");
        this.kafka.putMessage("OrderFulfillment", message);
    }

    protected void processFulfillmentResponse(OrderFulfillmentResponse response){
            SourcingRequest body = new SourcingRequest(response.order_id,
                                                    response.stockVisibilityResponse,
                                                    response,
                                                    response.stockVisibilityResponse.quantity);
            KafkaMessage msg = new KafkaMessage("Sourcing","SourcingRequest", body, this.getType(), "Sourcing");
            this.kafka.putMessage("Sourcing", msg);
        }


    protected synchronized void processSourcingResponse(SourcingResponse response)  {
        long finish_time =System.currentTimeMillis();
        try {

            File oFile = new File(this.path).getAbsoluteFile();
            if (!oFile.exists()) {
                oFile.createNewFile();
                log.info("crated file: "+this.path);
            }
            //log the information
            if (oFile.canWrite()) {
                this.file=  new BufferedWriter(new FileWriter(this.path,true));
                //order_id
                this.file.write(response.order_id.toString());
                this.file.write(",");
                //init time
                this.file.write(Long.toString(response.order_start));
                this.file.write(",");
                //finish time
                this.file.write(Long.toString(finish_time));
                this.file.write(",");
                //result
                this.file.write(Boolean.toString(response.success) + "\n");
                this.file.close();
            }
        }
        catch (IOException oException) {
            log.error("[ERROR] OrderManagement: Error appending/File cannot be written:" + this.path);

        }

    }


    public void run(){

        //process intermediate results until the end of the order responses
        while (!this.finish){
            KafkaMessage message = consumMessage();

            if (message!=null){
                log.debug("Current message:"+message.getType());
                if (message.getType()=="OrderManagementRequest")
                    this.processOrderRequest((OrderManagementRequest)message.getMessageBody());
                else if (message.getType()=="SourcingResponse")
                    this.processSourcingResponse((SourcingResponse) message.getMessageBody());
                else if (message.getType()=="StockVisibilityResponse")
                    this.processStockVisibilityResponse((StockVisibilityResponse) message.getMessageBody());
                else if (message.getType()=="OrderFulfillmentResponse")
                    this.processFulfillmentResponse((OrderFulfillmentResponse) message.getMessageBody());
                else if (message.getType()=="Kill") {
                    this.exit();
                    this.finish = true;
                }
                else
                    log.error("[ERROR] OrderManagemnet: unknow message "+message.getType());
            }
        }

    }

    //method from MicroService class. Not used here since run() has been re-implemented
    protected void processMessage(KafkaMessage message) {

    }

    protected void exit() {
        try {
                //this.bucket.close();
                log.info("OM - bucket closed");
                //this.cluster.disconnect();
                //log.info("OM - cluster disconnected");
                if (this.file != null )
                    this.file.close();
                log.info("OrderManagement exit");

            } catch (Exception e) {
                log.error(" Exit: "+ e.toString());
            }
    }

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
