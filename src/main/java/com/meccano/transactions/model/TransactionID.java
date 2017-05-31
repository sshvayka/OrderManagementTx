package com.meccano.transactions.model;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionID implements Serializable {

    private static AtomicLong counter = new AtomicLong(0);
    private final long id;

    public TransactionID() {
        id = counter.getAndIncrement();
    }

    public long getId() {
        return id;
    }

    public boolean equals(Object tid) {
        return ((TransactionID) tid).id == id;
    }

    public int hashCode() {
        return (int) id;
    }
}