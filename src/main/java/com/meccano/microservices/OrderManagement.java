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

    private void processOrderRequest(OrderManagementRequest request){
        // Create the StockVisibility request and send message to Kafka
        StockVisibilityRequest sr = new StockVisibilityRequest(request);
        KafkaMessage message = new KafkaMessage("StockVisibility", "StockVisibilityRequest", sr, this.getType(),"StockVisibility");
        super.getKafka().putMessage("StockVisibility", message);
    }

    private void processStockVisibilityResponse(StockVisibilityResponse response){
        // TO DO
        OrderFulfillmentRequest of = new OrderFulfillmentRequest(response);
        KafkaMessage message = new KafkaMessage("OrderFulfillment","OrderFulfillmentRequest", of, this.getType(),"OrderFulfillment");
        super.getKafka().putMessage("OrderFulfillment", message);
    }

    private void processFulfillmentResponse(OrderFulfillmentResponse response){
        SourcingRequest body = new SourcingRequest(response.getOrderId(),
                response.getStockVisibilityResponse(),
                response,
                response.getStockVisibilityResponse().getQuantity());
        KafkaMessage msg = new KafkaMessage("Sourcing","SourcingRequest", body, this.getType(), "Sourcing");
        super.getKafka().putMessage("Sourcing", msg);
    }

    private synchronized void processSourcingResponse(SourcingResponse response)  {
        long finish_time = System.currentTimeMillis();
        try {
            File oFile = new File(this.path).getAbsoluteFile();
            if(oFile.createNewFile()){
                log.info("Created file: " + this.path);
            } // Si el archivo ya existe, se usa
            // Log the information
            if (oFile.canWrite()) {
                this.file = new BufferedWriter(new FileWriter(this.path,true));
                // Order_id
                this.file.write(response.getOrderId().toString());
                this.file.write(",");
                // Init time
                this.file.write(Long.toString(response.getOrderStart()));
                this.file.write(",");
                // Finish time
                this.file.write(Long.toString(finish_time));
                this.file.write(",");
                // Result
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
            KafkaMessage message = consumeMessage();
            if (message != null) {
                log.debug("Current message:" + message.getType());
                switch (message.getType()) {
                    case "OrderManagementRequest":
                        this.processOrderRequest((OrderManagementRequest) message.getMessageBody());
                        break;
                    case "SourcingResponse":
                        this.processSourcingResponse((SourcingResponse) message.getMessageBody());
                        break;
                    case "StockVisibilityResponse":
                        this.processStockVisibilityResponse((StockVisibilityResponse) message.getMessageBody());
                        break;
                    case "OrderFulfillmentResponse":
                        this.processFulfillmentResponse((OrderFulfillmentResponse) message.getMessageBody());
                        break;
                    case "Kill":
                        this.exit();
                        super.setFinish(true);
                        break;
                    default:
                        log.error("[ERROR] OrderManagemnet: unknow message " + message.getType());
                }
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
            log.error(" Exit: " + e.toString());
        }
    }
}