package com.meccano.transactions;

//import org.onosproject.store.primitives.TransactionId;
import com.meccano.transactions.model.TransactionContext;
import com.meccano.transactions.model.TransactionID;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.UUID;

public class MainPrueba {

    public static void main(String[] args) {
        // Creating a random UUID (Universally unique identifier).
//        UUID uuid = UUID.randomUUID();
//        String randomUUIDString = uuid.toString();
//        TransactionId id = TransactionId.from(randomUUIDString);
//        System.out.println(id);

//        Timestamp timestamp = new Timestamp(Calendar.getInstance().getTime().getTime());
//        System.out.println(timestamp);

        TransactionContext ctx = new TransactionContext();
        System.out.println(ctx.getStateTransaction());
        ctx.setStateTransaction(TransactionContext.State.EXECUTING);
        System.out.println(ctx.getStateTransaction());
    }
}
