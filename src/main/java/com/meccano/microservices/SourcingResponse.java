package com.meccano.microservices;

import com.meccano.kafka.MessageBody;

import java.util.UUID;

public class SourcingResponse implements MessageBody {

    private UUID orderId;
    private boolean success;
    private long orderStart;

    public SourcingResponse(UUID orderId, boolean success){
        this.orderId = orderId;
        this.success = success;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getOrderStart() {
        return orderStart;
    }
}
