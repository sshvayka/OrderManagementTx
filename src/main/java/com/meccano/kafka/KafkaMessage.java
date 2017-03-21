package com.meccano.kafka;

/**
 * Base class for implementing the messages exchanged by the MS using Kafka broker
 */
public class KafkaMessage {
    private String type;
    private MessageBody body;
    private String source;
    private String destination;
    private String topic;

    public KafkaMessage(String topic, String type, MessageBody body, String source, String destination) {
        this.topic = topic;
        this.type = type;
        this.source = source;
        this.destination = destination;
        this.body = body;
    }

    public String getType(){
        return this.type;
    }

    public MessageBody getMessageBody (){
        return this.body;
    }

    public String getSource(){ return this.source;}

    public MessageBody getBody() {
        return body;
    }

    public String getDestination() {
        return destination;
    }

    public String getTopic() {
        return topic;
    }
}
