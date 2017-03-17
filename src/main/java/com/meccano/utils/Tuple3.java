package com.meccano.utils;

/**
 * Created by ruben.casado.tejedor on 06/09/2016.
 */
public class Tuple3<K,V,T> {

    private K e1;
    private V e2;
    private T e3;

    public Tuple3(K first, V second,T third ){
        this.e1 = first;
        this.e2 = second;
        this.e3 = third;
    }
}