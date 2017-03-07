package com.meccano.microservices;

import com.meccano.kafka.MessageBody;

import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 07/09/2016.
 */
public class SourcingResponse implements MessageBody {

    public UUID order_id;
    public boolean success;
    public long order_start;

    public SourcingResponse(UUID order_id, boolean success){
        this.order_id = order_id;
        this.success = success;
    }
}
