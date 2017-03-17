package com.meccano.microservices;

import com.meccano.kafka.KafkaBroker;
import com.meccano.kafka.KafkaMessage;
import com.meccano.utils.CBConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class OrderManagement extends MicroService {

    private BufferedWriter file;
    private String path;
    private static Logger log = LogManager.getLogger(OrderManagement.class);

    public OrderManagement(KafkaBroker kafka, CBConfig db, String path) throws IOException {
        super ("OrderManagement", kafka, "OrderManagement", db);
        this.path = path;
    }

    protected void processOrderRequest(OrderManagementRequest request){
        // Create the StockVisibility request and send message to Kafka
        StockVisibilityRequest sr = new StockVisibilityRequest(request);
        KafkaMessage message = new KafkaMessage("StockVisibility", "StockVisibilityRequest", sr, this.getType(),"StockVisibility");
        super.getKafka().putMessage("StockVisibility", message);
    }

    protected void processStockVisibilityResponse (StockVisibilityResponse response){
        // TO DO
        OrderFulfillmentRequest of = new OrderFulfillmentRequest(response);
        KafkaMessage message = new KafkaMessage("OrderFulfillment","OrderFulfillmentRequest", of, this.getType(),"OrderFulfillment");
        super.getKafka().putMessage("OrderFulfillment", message);
    }

    protected void processFulfillmentResponse(OrderFulfillmentResponse response){
            SourcingRequest body = new SourcingRequest(response.getOrder_id(),
                                                    response.getStockVisibilityResponse(),
                                                    response,
                                                    response.getStockVisibilityResponse().getQuantity());
            KafkaMessage msg = new KafkaMessage("Sourcing","SourcingRequest", body, this.getType(), "Sourcing");
            super.getKafka().putMessage("Sourcing", msg);
        }

    protected synchronized void processSourcingResponse(SourcingResponse response)  {
        long finish_time = System.currentTimeMillis();
        try {
            File oFile = new File(this.path).getAbsoluteFile();
            if (!oFile.exists()) {
                oFile.createNewFile();
                log.info("Created file: " + this.path);
            }
            // Log the information
            if (oFile.canWrite()) {
                this.file =  new BufferedWriter(new FileWriter(this.path,true));
                //order_id
                this.file.write(response.getOrder_id().toString());
                this.file.write(",");
                //init time
                this.file.write(Long.toString(response.getOrder_start()));
                this.file.write(",");
                //finish time
                this.file.write(Long.toString(finish_time));
                this.file.write(",");
                //result
                this.file.write(Boolean.toString(response.isSuccess()) + "\n");
                this.file.close();
            }
        }
        catch (IOException oException) {
            log.error("[ERROR] OrderManagement: Error appending/File cannot be written:" + this.path);
        }
    }

    public void run(){
        // Process intermediate results until the end of the order responses
        while (!super.isFinish()){
            KafkaMessage message = consumMessage();
            if (message != null){
                log.debug("Current message:" + message.getType());
                if (message.getType() == "OrderManagementRequest")
                    this.processOrderRequest((OrderManagementRequest)message.getMessageBody());
                else if (message.getType()=="SourcingResponse")
                    this.processSourcingResponse((SourcingResponse) message.getMessageBody());
                else if (message.getType()=="StockVisibilityResponse")
                    this.processStockVisibilityResponse((StockVisibilityResponse) message.getMessageBody());
                else if (message.getType()=="OrderFulfillmentResponse")
                    this.processFulfillmentResponse((OrderFulfillmentResponse) message.getMessageBody());
                else if (message.getType()=="Kill") {
                    this.exit();
                    super.setFinish(true);
                } else
                    log.error("[ERROR] OrderManagemnet: unknow message " + message.getType());
            }
        }
    }

    //Method from MicroService class. Not used here since run() has been re-implemented
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
            super.getDb().getCluster().disconnect();
        } catch (Exception e) {
            log.error(" Exit: "+ e.toString());
        }
    }
}
