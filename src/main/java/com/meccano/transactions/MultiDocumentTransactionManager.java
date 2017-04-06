package com.meccano.transactions;

import com.couchbase.client.java.Bucket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class MultiDocumentTransactionManager {

    private static final int OPTIMISTIC = 0;
    private static final int PESSIMISTIC = 1;
    private Protocol protocol;

    // Centralized Logs (at all implementations)
    private static Logger log = LogManager.getLogger(MultiDocumentTransactionManager.class);

    public MultiDocumentTransactionManager (int type, Bucket bucket){
        switch(type) {

            case OPTIMISTIC:
                this.protocol = new Optimistic(bucket);
                log.info("Selected Optimistic Mode");
                break;

            case PESSIMISTIC:
                this.protocol = new Pessimistic(bucket);
                log.info("Selected Pessimistic Mode");
                break;

            default:
                log.info("Selected Default Mode");
        }
    }

    public void begin(List<String> docIds) {
        this.protocol.begin(docIds);
    }

    public void rollback() {
        this.protocol.rollback();
    }

    public void commit() {
        this.protocol.commit();
    }

    public String getState() {
        return this.protocol.getState();
    }

    public void updateState(String state) {
        this.protocol.updateState(state);
    }

    public void close() {
        this.protocol.close();
    }


}
