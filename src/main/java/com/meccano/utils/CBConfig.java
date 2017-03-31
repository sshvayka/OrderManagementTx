package com.meccano.utils;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

/**
 * Class to encapsulate the connexion parameters for Couchbase.
 * In next releases, they should be parameters of the constructor
 */
public class CBConfig {

    private CouchbaseCluster cluster;
    private Bucket bucket;
    private String clusterURL;
    private String bucketName;
    private String password;
//    private CouchbaseEnvironment env = DefaultCouchbaseEnvironment.create();

    public CBConfig(String clusterURL, String bucketName){
        this.clusterURL = clusterURL;
        this.bucketName = bucketName;
        this.password = null;
        this.cluster = CouchbaseCluster.create(this.clusterURL);
        this.bucket = cluster.openBucket(bucketName);
    }

    public CBConfig(String clusterURL, String bucketName, String pass){
        this.clusterURL = clusterURL;
        this.bucketName = bucketName;
        this.password = pass;
        this.cluster = CouchbaseCluster.create(this.clusterURL);
        // Connect to the bucket and open it
        if (pass != null)
            this.bucket = cluster.openBucket(bucketName, pass);
        else
            this.bucket = cluster.openBucket(bucketName);
    }

    public CouchbaseCluster getCluster() {
        return cluster;
    }

    public String getBucketName() {
        return bucketName;
    }

    public Bucket getBucket(){
        return bucket;
    }

    public String getPassword() {
        return password;
    }

    public void exit (){
        cluster.disconnect();
    }
}
