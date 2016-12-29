package com.meccano.microservices;

import com.meccano.utils.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.UUID;

/**
 * Created by ruben.casado.tejedor on 30/08/2016.
 */
public class StockVisibilityResponse implements com.meccano.kafka.MessageBody{

    static Logger log = Logger.getLogger(StockVisibilityResponse.class.getName());

    public StockVisibilityResponse (UUID order_id, ArrayList<String> stock_id, ArrayList<Pair<String, Integer>> quantity){
        this.stock_id=stock_id;
        this.order_id=order_id;
        this.stocks = new Hashtable<String, ArrayList<Pair<String,Integer>>> ();
        Iterator<String> itr= stock_id.iterator();
        while (itr.hasNext()){
            ArrayList<Pair<String, Integer>> aux = new ArrayList<Pair<String, Integer>>();
            this.stocks.put(itr.next(), aux);
        }
        //items and quantity required
        this.quantity=quantity;
    }

    public UUID order_id;
    public ArrayList<Pair<String, Integer>> quantity;
    protected ArrayList<String> stock_id;
    public Hashtable<String, ArrayList<Pair<String,Integer>>> stocks;

    protected void add(String item_id, Pair<String,Integer> p){

        ArrayList<Pair<String,Integer>> temp = this.stocks.get(item_id);
        if (temp!= null){
            temp.add(p);
            stocks.put(item_id,temp);
        }
        else
            log.error("Error in ADD method - there is no item : "+item_id);


    }
}
