package com.meccano;

import com.couchbase.client.core.CouchbaseException;
import com.couchbase.client.java.error.FlushDisabledException;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.meccano.utils.CBConfig;
import com.meccano.utils.CBDataGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UtilsCouchbase {

    private static Logger log = LogManager.getLogger(UtilsCouchbase.class);

    public static void main(String[] args) {
        String host = "localhost";
        String buckName = "mecanno";
        // Details for couchbase connection. Localhost default
        CBConfig db = new CBConfig (host, buckName);

        // Flush previous data
        if(flushBucket(db)){
            log.info("Flush successful");
        } else {
            log.error("Flush error");
            return;
        }

        // Insert initial data
        insertData(db, 500, 100);
        
        getNumOfData(db);
    }

    private static void insertData (CBConfig db, int num, int variety){
        // Code for generating fake data in Couchbase
        CBDataGenerator generator = new CBDataGenerator(db);
        generator.createItems(num, variety);
        log.info("Random items created");
    }

    private static void getNumOfData (CBConfig db){
        N1qlQueryResult result = db.getBucket().query(
                N1qlQuery.simple("SELECT COUNT(*) FROM " + db.getBucketName()));
        log.info("Inserted elements: " + result.allRows().get(0).value().get("$1"));
    }

    private static boolean flushBucket (CBConfig db){
        boolean flush = false;
        try {
            flush = db.getBucket().bucketManager().flush();
        } catch (FlushDisabledException d){
            log.error("Flush is disabled for bucket \"" + db.getBucketName() + "\". Abort of operation");
        } catch (CouchbaseException c){
            log.error("Server response could not be parsed");
        }
        return flush;
    }
}
