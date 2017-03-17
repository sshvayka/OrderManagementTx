package com.meccano.microservices;

import com.meccano.kafka.MessageBody;

import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 07/09/2016.
 */
public class SourcingResponse implements MessageBody {

    private UUID order_id;
    private boolean success;
    private long order_start;

    public SourcingResponse(UUID order_id, boolean success){
        this.order_id = order_id;
        this.success = success;
    }

    public UUID getOrder_id() {
        return order_id;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getOrder_start() {
        return order_start;
    }
}
