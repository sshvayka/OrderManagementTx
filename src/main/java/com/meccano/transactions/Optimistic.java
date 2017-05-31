package com.meccano.transactions;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class Optimistic implements Protocol{

    private Bucket bucket;
    private String state;

    private Map<String, JsonDocument> originalDocs; // List containing original docs ids to create/mutate
    private Map<String, JsonDocument> updatedDocs;  // List containing updated docs
    private Map<String, JsonDocument> blockedDocs;  // List containing locked files

    private static Logger log = LogManager.getLogger(MultiDocumentTransactionManager.class);

    public Optimistic (Bucket bucket){
        this.bucket = bucket;
    }

    @Override
    public void begin(List<String> docsIds) {
        updateState("START");
        updateState("LOCK");
        lockDocuments(docsIds);
        updateState("LOCKED");
    }

    @Override
    public void partialCommit(JsonDocument doc) {
        updateState("PARTIAL_COMMIT");
        updatedDocs.put(doc.id(), doc);
        updateState("PARTIAL_COMMITED");
    }

    @Override
    public void commit() {
        this.state = "COMMIT";
        unlockDocuments(blockedDocs);
        this.state = "COMMITED";
    }

    @Override
    /* TODO en caso de que se haya modificado un documento, a la hora de hacer abort, si voy a restaurar un documento que ha sido modificado en Couchbase, problema. Hay que lanzar excepcion, no se puede salvar */
    /* TODO decidir si repetir transaccion entera en ese caso */
    public void abort() {
        this.state = "ABORT";
        for (String updatedDocId : updatedDocs.keySet()){
            JsonDocument origDoc = originalDocs.get(updatedDocId);
            if (origDoc != null)
                bucket.upsert(origDoc);
            else
                log.error("Abort error"); // TODO si falla el abort, ¿seguir o abortar?
        }
        unlockDocuments(blockedDocs);
    }

    @Override
    public void close() {
        updateState("CLOSED");
        bucket.close();
    }

    @Override
    public String getState() {
        return this.state;
    }

    @Override
    public void updateState(String state) {
        this.state = state;
    }

    private void unlockDocuments (Map<String, JsonDocument> blocks){
        for (String blockId : blocks.keySet()) {
            if (bucket.exists(blockId)) {
                try {
                    bucket.remove(blockId);
                } catch (Exception e){
                    log.info("Error at deleting lock document");
                }
            } else {
                log.error("Document to unlock doesn't exist");
            }
        }
    }

    private void lockDocuments (List<String> docsIds){
        for (String docId : docsIds){
            if(bucket.exists(docId)){
                try {
                    JsonDocument origDoc = bucket.get(docId);
                    originalDocs.put(origDoc.id(), origDoc);
                    bucket.insert(JsonDocument.create(docId + "_lock", origDoc.content()));
                    log.info("Document successfully locked");
                } catch (Exception e){
                    log.error("Error at creating lock file. Abort");
                    unlockDocuments(blockedDocs);
                }
            } else {
                log.error("Document to lock doesn't exist. Abort");
                unlockDocuments(blockedDocs);
            }
        }
    }
}
