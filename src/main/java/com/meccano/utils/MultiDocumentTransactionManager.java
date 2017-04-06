package com.meccano.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class MultiDocumentTransactionManager {

    private CBConfig db;
    private CouchbaseCluster cluster;
    private Bucket bucket; // TODO crear bucket en cada clase que lo use
    private ArrayList<String> docsIds;
    private ArrayList<JsonDocument> originalDocs;
    private ArrayList<String> updatedIds;
//    private long timeout;
//    private String owner;
    private String state;

    private static Logger log = LogManager.getLogger(MultiDocumentTransactionManager.class);

    public MultiDocumentTransactionManager(CBConfig db){
        this.db = db;
        this.docsIds = new ArrayList<>();
        this.originalDocs = new ArrayList<>();
        this.updatedIds = new ArrayList<>();
//        this.timeout = 100;
        this.state = "INITIAL";

        if (this.db == null) {
            log.error("[ERROR] MultiDocumentTransactionManager: CBConfig is null");
        } else {
            // Create a cluster reference
            this.cluster = db.getCluster();
            // Create a bucket reference
            this.bucket = db.getBucket();
        }
    }

    public void partialCommit(String docId){
        this.updatedIds.add(docId);
        this.state = "PARTIAL_COMMIT";
    }

    public void commit(){
        this.removeLockDocuments();
        this.state = "COMMITTED";
    }

    public void close(){
        this.bucket.close();
    }

    protected JsonDocument getOriginal(String docId){
        JsonDocument result = null;
        for (JsonDocument origDoc : this.originalDocs) {
            if (origDoc.id().equals(docId)) {
                result = origDoc;
                break;
            }
        }
        return result;
    }

    public void rollback(){
        for (String updated_id : updatedIds) {
            JsonDocument origDoc = this.getOriginal(updated_id);
            if (origDoc != null)
                bucket.upsert(origDoc);
            else
                log.error("[ERROR] MultiDocumentTransactionManager: Rollback error");
        }
        this.state = "ROLLBACKED";
    }

    private void removeLockDocuments(){
        for (JsonDocument origDoc : originalDocs) {
            String origId = origDoc.id();
            String lockId = origId + "_lock";
//            log.info("LOCK a borrar: " + origId + "_lock");
            if (this.bucket.exists(lockId)) {
                try {
                    this.bucket.remove(lockId);
                } catch (DocumentDoesNotExistException e){
//                    log.error("Lock document \"" + lockId + "\" already deleted");
                }
            }
        }
    }

    public void start(){
        this.state = "STARTED";
    }

    public boolean createLockDocument(String docId){
        boolean result;
        try {
            JsonDocument origDoc = bucket.get(docId);
            originalDocs.add(origDoc);
            JsonObject contOrigDoc = origDoc.content();
            JsonDocument lockDoc = JsonDocument.create(docId + "_lock", contOrigDoc);

            bucket.upsert(lockDoc);
            log.debug("[TX] Saved document " + lockDoc.id());
            result = true;
        } catch (Exception e){
            log.error("[ERROR] MultiDocumentTransactionManager: " + docId + " already blocked");
            result = false;
        }
        return result;
    }

    public String getState(){
        return this.state;
    }

    public void setState(String state){
        this.state = state;
    }
}