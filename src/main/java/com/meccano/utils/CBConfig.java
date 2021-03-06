package com.meccano.utils;

import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * Class to encapsulate the connexion parameters for Couchbase.
 * In next releases, they should be parameters of the constructor
 */
public class CBConfig {

    private CouchbaseCluster cluster;
    private String clusterURL;
    private String bucket;
    private String password;
    private CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();

    public CBConfig(){
        this.clusterURL = "localhost";
        this.bucket = "default";
        this.password = null;
        this.cluster = CouchbaseCluster.create(env, this.clusterURL);
    }

    public CBConfig(String clusterURL, String bucket, String pass){
        this.clusterURL = clusterURL;
        this.bucket = bucket;
        this.password = pass;
        this.cluster = CouchbaseCluster.create(env, this.clusterURL);
    }

    public CouchbaseCluster getCluster() {
        return cluster;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPassword() {
        return password;
    }

    public void exit (){
        cluster.disconnect();
        env.shutdown();
    }
}
