package com.meccano.transactions;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Pessimistic implements Protocol{

    private Bucket bucket;      // Connected Bucket
    private String state;       // State of transaction
    private int lockingTime;    // Time to lock a file
//    private String owner;       // Owner of the transaction

    private Map<String, JsonDocument> originalDocs; // List containing original docs ids to create/mutate
    private Map<String, JsonDocument> updatedDocs;  // List containing updated docs
    private Map<String, JsonDocument> blockedDocs;  // List containing locked files

    private static Logger log = LogManager.getLogger(MultiDocumentTransactionManager.class);

    /**
     * Default builder
     * @param bucket
     */
    public Pessimistic (Bucket bucket){
        this.bucket = bucket;
        this.lockingTime = 30; // By default, maximum locking time allowed
//        this.owner = owner;

        this.originalDocs = new HashMap<>();
        this.updatedDocs = new HashMap<>();
        this.blockedDocs = new HashMap<>();
    }

    //TODO otro constructor con parametros por constructor

    @Override
    public void begin (List<String> docsIds) {
        updateState("START");
        updateState("LOCK");
        lockDocuments(docsIds);
        updateState("LOCKED");
    }

    @Override
    public void partialCommit (JsonDocument doc) {
        updateState("PARTIAL_COMMIT");
        updatedDocs.put(doc.id(), doc);
        updateState("PARTIAL_COMMITED");
    }

    @Override
    public void commit () {
        this.state = "COMMIT";
        unlockDocuments(blockedDocs);
        this.state = "COMMITED";
    }

    @Override
    public void abort () {
        this.state = "ABORT";
        for (String updatedDocId : updatedDocs.keySet()) {
            JsonDocument origDoc = originalDocs.get(updatedDocId);
            if (origDoc != null)
                bucket.upsert(origDoc);
            else
                log.error("Abort error"); // TODO si falla el abort, ¿seguir o abortar?
        }
        this.state = "ABORTED";
    }

    @Override
    public void close () {
        updateState("CLOSED");
        bucket.close();
    }

    @Override
    public String getState () {
        return this.state;
    }

    @Override
    public void updateState (String state) {
        this.state = state;
    }

    private void unlockDocuments (Map<String, JsonDocument> blocks){
        for (JsonDocument block : blocks.values()) {
            if (bucket.exists(block)) {
                try {
                    this.bucket.unlock(block);
                } catch (Exception e){
                    // TODO ¿cancelar la operacion o seguir?
                    log.error("Error at unlock operation");
                }
            } else {
                // TODO ¿cancelar la operacion o seguir?
                log.error("Document to unlock doesn't exist");
            }
        }
    }

    private void lockDocuments (List<String> docsIds){
        for (String docId : docsIds) {
            if (bucket.exists(docId)) {
                try {
                    JsonDocument blockedDoc = bucket.getAndLock(docId, lockingTime);
                    originalDocs.put(docId, blockedDoc); // Se supone que el GetAndLock devuelve el JsonDocument original
                    blockedDocs.put(docId, blockedDoc);
                    log.info("Document successfully locked");
                } catch (Exception e) {
                    log.error("Error at GetAndLock operation. Abort");
                    unlockDocuments(blockedDocs);
                }
            } else {
                log.error("Document to lock doesn't exist. Abort");
                unlockDocuments(blockedDocs);
            }
        }
    }
}
