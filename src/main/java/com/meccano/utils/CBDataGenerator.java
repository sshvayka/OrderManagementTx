package com.meccano.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

import java.util.*;

public class CBDataGenerator {

    private CBConfig db;
    private CouchbaseCluster cluster;
    private Bucket bucket;
    private Random rd;
    private int variety = 100;

    public CBDataGenerator(CBConfig db) {
        this.db = db;
        connect();
        rd = new Random(System.currentTimeMillis());
    }

    public void close(){
        cluster.disconnect();
    }

    public void createItems(int num, int variety){
        JsonObject item;
        String itemId;
        String storeId;
        this.variety = variety;
        for (int i = 0; i < num; i++){
            itemId = getRandomItem(variety);
            storeId = getRandomStore();
            item = JsonObject.create()
                    .put("_type", "stock")
                    .put("itemId", itemId)
                    .put("storeId", storeId)
                    .put("quantity", rd.nextInt(100) + 1)
                    .put("price", rd.nextInt(49) + 1)
                    .put("currency", "E")
                    .put("category", getRandomCategories());
            JsonDocument doc = JsonDocument.create(storeId + "-" + itemId, item);
            bucket.upsert(doc);
        }
    }

    private JsonArray getRandomCategories(){
        int number = rd.nextInt(3) + 1;
        Set<String> set = new TreeSet<>();

        for (int i = 0; i < number; i++) {
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
            }
        }
        List<String> categories= new ArrayList<>();
        for (String aSet : set)
            categories.add(aSet);
        return JsonArray.from(categories);
    }

    private String getRandomItem(int variety)
    {
        return Integer.toString(rd.nextInt(variety) + 1);
    }

    public void createOrders(int num) {
        for (int i = 0; i < num; i++) {
            int order_id = Math.abs(rd.nextInt());
            String state = this.getRandomOrderState();
            // create order details
            JsonObject order = JsonObject.create()
                    .put("_type", "Order")
                    .put("orderId", Integer.toString(order_id))
                    .put("state", state);
            //create suborders
            JsonArray suborders = JsonArray.create();
            for (int j = 0; j < rd.nextInt(3) + 1; j++) {
                JsonObject suborder = JsonObject.create()
                        .put("suborderId", Integer.toString(rd.nextInt()))
                        .put("storeId", getRandomStore())
                        .put("state", getRandomSuborderState());
                //create items for each suborder
                JsonArray items = JsonArray.create();
                for (int k = 0; k < rd.nextInt(5) + 1; k++) {
                    JsonObject item = JsonObject.create()
                            .put("itemId", Integer.toString(rd.nextInt(variety)+1))
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
            bucket.upsert(doc);
        }
    }

    public void changeConfig(CBConfig db) {
        cluster.disconnect();
        this.db = db;
        connect();
    }

    private void connect() {
        if (this.db == null) {
            System.err.println("[ERROR] CBDataGenerator: CBConfig is null");
        } else {
            // Create a cluster reference
            this.cluster = this.db.getCluster();
            // Create a bucket reference
            this.bucket = this.db.getBucket();
        }
    }

    public String getRandomOrderState() {
        // nextInt((max - min) + 1) + min
        int x = rd.nextInt(3);  // (((2 - 0) + 1) + 0)
        String orderState;
        switch (x) {
            case 0:
                orderState = "PRE-AUTHORIZE";
                break;
            case 1:
                orderState = "AUTHORIZED";
                break;
            case 2:
                orderState = "PAID";
                break;
            default:
                orderState = null;
        }
        return orderState;
    }

    public String getRandomSuborderState() {
        // nextInt((max - min) + 1) + min
        int x = rd.nextInt(2);  // (((1 - 0) + 1) + 0)
        String suborderState;
        switch (x) {
            case 0:
                suborderState = "ALLOCATED";
                break;
            case 1:
                suborderState = "PICKED";
                break;
            default:
                suborderState = null;
        }
        return suborderState;
    }

    public String getRandomStore() {
        // nextInt((max - min) + 1) + min
        int x = rd.nextInt(5);  // (((4 - 0) + 1) + 0)
        String store;
        switch (x) {
            case 0:
                store = "Gijon";
                break;
            case 1:
                store = "Madrid";
                break;
            case 2:
                store = "Burgos";
                break;
            case 3:
                store = "Nancy";
                break;
            case 4:
                store = "Oxford";
                break;
            default:
                store = null;
        }
        return store;
    }

    public Bucket getBucket() {
        return bucket;
    }
}