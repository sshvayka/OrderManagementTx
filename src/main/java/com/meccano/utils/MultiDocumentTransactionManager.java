package com.meccano.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class MultiDocumentTransactionManager {

    private CBConfig db;
    private CouchbaseCluster cluster;
    private Bucket bucket;
    private ArrayList<String> docsIds;
    private ArrayList<JsonDocument> originalDocs;
    private ArrayList<String> updatedIds;
//    private long timeout;
//    private String owner;
    private String state;

    private static Logger log = LogManager.getLogger(MultiDocumentTransactionManager.class.getName());

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
            cluster = this.db.getCluster();
            // Connect to the bucket and open it
            if (db.getPassword() != null)
                bucket = cluster.openBucket(this.db.getBucket(), this.db.getPassword());
            else
                bucket = cluster.openBucket(this.db.getBucket());
        }
    }

    public void partialCommit(String document_id){
        this.updatedIds.add(document_id);
        this.state = "PARTIAL_COMMIT";
    }

    public void commit(){
        this.removeLockDocuments();
        this.state = "COMMITTED";
    }

    public void close(){
        this.bucket.close();
    }

    protected JsonDocument getOriginal(String id){
        JsonDocument result = null;
        for (JsonDocument d : this.originalDocs) {
            if (d.id().equals(id)) {
                result = d;
                break;
            }
        }
        return result;
    }

    public void rollback(){
        for (String updated_id : updatedIds) {
            JsonDocument original = this.getOriginal(updated_id);
            if (original != null)
                bucket.upsert(original);
            else
                log.error("[ERROR] MultiDocumentTransactionManager: Rollback error");
        }
        this.state = "ROLLBACKED";
    }

    private void removeLockDocuments(){
        for (JsonDocument original_document : originalDocs) {
            String original_id = original_document.id();
            this.bucket.remove(original_id + "_lock");
        }
    }

    public void start(){
        this.state = "STARTED";
    }

    public boolean createLockDocument(String document_id){
        boolean result = true;
        JsonDocument found = bucket.get(document_id);
        originalDocs.add(found);
//        JsonObject object = found.content();

        JsonDocument lock_document = JsonDocument.create(document_id + "_lock");
        try {
            bucket.upsert(lock_document);
            log.debug("[TX] Saved document "+ lock_document.id());
        } catch (Exception e){
            log.error("[ERROR] MultiDocumentTransactionManager: " + document_id + " already blocked");
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