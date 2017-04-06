package com.meccano.transactions;

import com.couchbase.client.java.document.JsonDocument;

import java.util.List;

public interface Protocol {

    void begin (List<String> docsIds);

    void partialCommit (JsonDocument doc);

    void commit ();

    void rollback ();

    void close ();

    String getState ();

    void updateState (String state);
}
