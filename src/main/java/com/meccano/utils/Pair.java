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
        boolean match;
        if (!(obj instanceof Pair))
            match = false;
        else if (obj == this)
            match = true;
        else {
            Pair p = (Pair) obj;
            match = this.key.equals(p.key) && this.value.equals(p.value);
        }
        return match;
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