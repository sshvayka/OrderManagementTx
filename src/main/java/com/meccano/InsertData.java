package com.meccano;

import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.meccano.utils.CBConfig;
import com.meccano.utils.CBDataGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class InsertData {

    private static Logger log = LogManager.getLogger(InsertData.class);
    private static String BUCKET = "mecanno";

    public static void main(String[] args) {
        // Details for couchbase connection. Localhost default
        CBConfig db = new CBConfig();
        db.setBucket(InsertData.BUCKET);

        // Code for generating fake data in Couchbase
        CBDataGenerator generator = new CBDataGenerator(db);
        generator.createItems(500, 100);
//        generator.createOrders(500);
//        generator.close();
        log.info("Random items created");

        N1qlQueryResult result = generator.getBucket().query(
                N1qlQuery.simple("SELECT COUNT(*) FROM " + db.getBucket()));

//        int num = 0;
//        // Para cada id, borrar el doc
//        for (N1qlQueryRow row : result) {
//            System.out.println("ROW: " + row);
//            num++;
//        }
        System.out.println("Elementos insertados: " + result.allRows().get(0).value().get("$1"));

    }
}
