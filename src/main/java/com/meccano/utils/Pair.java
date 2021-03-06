package com.meccano.utils;

public class Pair<K,V> {

    private K key;
    private V value;

    public Pair (K key, V value){
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof Pair))
            return false;
        if (obj == this)
            return true;
        Pair p = (Pair) obj;
        return this.key.equals(((Pair) obj).key) && this.value.equals(((Pair) obj).value);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.key.hashCode();
        result = 31 * result + this.value.hashCode();
        return result;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }
}