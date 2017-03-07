package com.meccano.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

/**
 * Created by ruben.casado.tejedor on 07/09/2016.
 */
public class MultiDocumentTransactionManager {

    protected CBconfig db;
    protected CouchbaseCluster cluster;
    protected Bucket bucket;
    protected ArrayList<String> document_ids;
    protected ArrayList<JsonDocument> original_documents;
    protected ArrayList<String> updated_ids;
    protected long timeout;
    protected String owner;
    protected String state;

    static Logger log = LogManager.getLogger(MultiDocumentTransactionManager.class.getName());

    public MultiDocumentTransactionManager(CBconfig db){
        document_ids = new ArrayList<String>();
        original_documents = new ArrayList<JsonDocument>();
        updated_ids = new ArrayList<String>();

        timeout = 100;
        state = new String("INITIAL");
        this.db = db;

        if (this.db == null) {
            log.error("[ERROR] MultiDocumentTransactionManager: CBconfig is null");
            return;
        }

        // Create a cluster reference
        cluster = this.db.cluster;
        // Connect to the bucket and open it
        if (db.password != null)
            bucket = cluster.openBucket(this.db.bucket, this.db.password);
        else
            bucket = cluster.openBucket(this.db.bucket);
    }

    public void partialCommit(String document_id){
        this.updated_ids.add(document_id);
        this.state = "PARTIAL_COMMIT";
    }

    public void updateState(String state){
        this.state = state;
    }

    public String getState(){
        return this.state;
    }

    public void commit(){
        this.removeLockDocuments();
        this.state = "COMMITTED";
    }

    public void close(){
        this.bucket.close();
    }

    protected JsonDocument getOriginal(String id){
        for (int i = 0; i < this.original_documents.size(); i++){
            JsonDocument d = this.original_documents.get(i);
            if (d.id() == id)
                return d;
        }
        return null;
    }

    public void rollback(){
        for (int i = 0; i < updated_ids.size(); i++){
           JsonDocument original = this.getOriginal(updated_ids.get(i));
            if (original != null)
                bucket.upsert(original);
            else
                log.error("[ERROR] MultiDocumentTransactionManager: Rollback error");
        }
        this.state = "ROLLBACKED";
    }

    protected void removeLockDocuments(){
        for (int i = 0; i < original_documents.size(); i++){
            String original_id = original_documents.get(i).id();
            this.bucket.remove(original_id + "_lock");
        }
    }

    public void start(){
        this.state = "STARTED";
    }

    public boolean createLockDocument(String document_id){
        JsonDocument found = bucket.get(document_id);
        original_documents.add(found);
        JsonObject object = found.content();

        JsonDocument lock_document = JsonDocument.create(document_id + "_lock");
        try {
            bucket.upsert(lock_document);
            log.debug("[TX] Saved document "+ lock_document.id());
        }
        catch (Exception e){
            log.error("[ERROR] MultiDocumentTransactionManager: " + document_id + " already blocked");
            return false;
        }
        return true;
    }
}