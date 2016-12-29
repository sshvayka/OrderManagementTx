package com.meccano.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.*;

/**
 * Created by ruben.casado.tejedor on 31/08/2016.
 */
public class CBDataGenerator {

    protected CBconfig db;
    protected CouchbaseCluster cluster;
    protected Bucket bucket;
    protected Random rd;
    protected int variety =100;
    public CBDataGenerator(CBconfig db) {
        this.db = db;
        connect();
        rd = new Random(System.currentTimeMillis());
    }
    public void close(){
        cluster.disconnect();
    }
    public void createItems(int num, int variety){
        JsonObject item;
        String item_id;
        String store_id;
        this.variety=variety;
        for (int i=0; i<num; i++){
            item_id=getRandomItem(variety);
            store_id=getRandomStore();
            item = JsonObject.create()
                    .put("_type", "stock")
                    .put("item_id", item_id)
                    .put("store_id", store_id)
                    .put("quantity", rd.nextInt(100)+1)
                    .put("price", rd.nextInt(49)+1)
                    .put("currency", "E")
                    .put("category", getRandomCategories());
            JsonDocument doc = JsonDocument.create(store_id+"-"+item_id, item);
            JsonDocument inserted = bucket.upsert(doc);
        }

    }
    protected JsonArray getRandomCategories(){
        int number= rd.nextInt(3)+1;
        Set<String> set = new TreeSet<String>();

        for (int i=0; i<number; i++) {
            int c = rd.nextInt(6);
            switch (c) {
                case 0:
                    set.add("HOMBRE");
                    break;
                case 1:
                    set.add("MUJER");
                    break;
                case 2:
                    set.add("VERANO");
                    break;
                case 3:
                    set.add("INVIERNO");
                    break;
                case 4:
                    set.add("CAMISA");
                    break;
                case 5:
                    set.add("PANTALON");
                    break;
                default:
                    set.add("MODA");
                    break;
            }
        }
        List<String> categories= new ArrayList<String>();
        Iterator<String> itr = set.iterator();
        while (itr.hasNext())
            categories.add(itr.next());
        return JsonArray.from(categories);
    }

    protected String getRandomItem(int variety)
    {
        return Integer.toString(rd.nextInt(variety)+1);
    }

    public void createOrders(int num) {
        for (int i = 0; i < num; i++) {
            int order_id = Math.abs(rd.nextInt());
            String state = this.getRandomOrderState();
            // create order details
            JsonObject order = JsonObject.create()
                    .put("_type", "Order")
                    .put("order_id", Integer.toString(order_id))
                    .put("state", state);
            //create suborders
            JsonArray suborders = JsonArray.create();
            for (int j = 0; j < rd.nextInt(3) + 1; j++) {
                JsonObject suborder = JsonObject.create()
                        .put("suborder_id", Integer.toString(rd.nextInt()))
                        .put("store_id", getRandomStore())
                        .put("state", getRandomSuborderState());
                //create items for each suborder
                JsonArray items = JsonArray.create();
                for (int k = 0; k < rd.nextInt(5) + 1; k++) {
                    JsonObject item = JsonObject.create()
                            .put("item_id", Integer.toString(rd.nextInt(variety)+1))
                            .put("price", Float.toString(rd.nextFloat() + 1))
                            .put("currency", "E")
                            .put("quantity", rd.nextInt(2) + 1);
                    items.add(item);
                }
                suborder.put("items", items);
                suborders.add(suborder);
            }
            order.put("suborders", suborders);
            JsonDocument doc = JsonDocument.create(Integer.toString(order_id), order);
            JsonDocument inserted = bucket.upsert(doc);
        }
    }

    public void changeConfig(CBconfig db) {
        cluster.disconnect();
        this.db = db;
        connect();
    }

    protected void connect() {
        if (this.db == null) {
            System.err.println("[ERROR] CBDataGenerator: CBconfig is null");
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

    public String getRandomOrderState() {
        // nextInt((max - min) + 1) + min
        int x = rd.nextInt(2 - 0 + 1) + 0;

        switch (x) {
            case 0:
                return "PRE-AUTHORIZE";
            case 1:
                return "AUTHORIZED";
            case 2:
                return "PAID";
        }
        return null;
    }

    public String getRandomSuborderState() {
        // nextInt((max - min) + 1) + min
        int x = rd.nextInt(1 - 0 + 1) + 0;

        switch (x) {
            case 0:
                return "ALLOCATED";
            case 1:
                return "PICKED";
        }
        return null;
    }

    public String getRandomStore() {
        // nextInt((max - min) + 1) + min
        int x = rd.nextInt(4 - 0 + 1) + 0;

        switch (x) {
            case 0:
                return "Gijon";
            case 1:
                return "Madrid";
            case 2:
                return "Burgos";
            case 3:
                return "Nancy";
            case 4:
                return "Oxford";
        }
        return null;
    }
}



