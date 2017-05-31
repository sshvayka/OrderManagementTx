package com.meccano.transactions.model;

import com.meccano.transactions.Protocol;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TransactionContext {

    public enum State {
        STARTED, EXECUTING, COMMITED, ABORTED, CLOSED
    }

    private TransactionID idTransaction;
    private String idCreator;
    private List<TransactionID> listIds;
    private Timestamp timestamp;
    private int tMaxLife;
    private int numIntentos;
    private Protocol protocol;
    private State stateTransaction;

    public TransactionContext(){}

    public TransactionContext(TransactionID idTransaction, String idCreator, int tMaxLife, int numIntentos, Protocol protocol) {
        this.idTransaction = idTransaction;
        this.idCreator = idCreator;
        this.listIds = new ArrayList<>();
        this.timestamp = new Timestamp(Calendar.getInstance().getTime().getTime());
        this.tMaxLife = tMaxLife;
        this.numIntentos = numIntentos;
        this.protocol = protocol;
        this.stateTransaction = State.STARTED;
    }

    // Getters and Setters
    public TransactionID getIdTransaction() {
        return idTransaction;
    }

    public void setIdTransaction(TransactionID idTransaction) {
        this.idTransaction = idTransaction;
    }

    public String getIdCreator() {
        return idCreator;
    }

    public void setIdCreator(String idCreator) {
        this.idCreator = idCreator;
    }

    public List<TransactionID> getListIds() {
        return listIds;
    }

    public void setListIds(List<TransactionID> listIds) {
        this.listIds = listIds;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public int gettMaxLife() {
        return tMaxLife;
    }

    public void settMaxLife(int tMaxLife) {
        this.tMaxLife = tMaxLife;
    }

    public int getNumIntentos() {
        return numIntentos;
    }

    public void setNumIntentos(int numIntentos) {
        this.numIntentos = numIntentos;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public State getStateTransaction() {
        return stateTransaction;
    }

    public void setStateTransaction(State stateTransaction) {
        this.stateTransaction = stateTransaction;
    }
}
