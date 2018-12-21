package com.rishabh.core;


import java.util.concurrent.ThreadLocalRandom;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

public class ClientThread extends Thread {

    private MongoClient mongoConn;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    private int threadNumber;
    private long batch;

    public ClientThread(long batch, int threadNumber) {
        this.batch = batch;
        this.threadNumber = threadNumber;
    }

    public void run() {
        this.mongoConn = new MongoClient("10.10.1.202", 27017);
        this.database = this.mongoConn.getDatabase("test");
        this.collection = this.database.getCollection("mycol");
        long rand = this.getRandomNumber();
        this.process_cursor(rand);
        // System.out.println("Connection opened to MongoD instance running at localhost:27017");
        this.cleanup();
    }

    private void process_cursor(long skip) {
        // System.out.println("Read the database skipping " + String.valueOf(skip) +" and limiting " + String.valueOf(this.batch));
        ClientThread clientThread = this;
        int batch_size = (int) clientThread.batch;
        int skip_ind = (int) skip;
        FindIterable<Document> findIterable = clientThread.collection.find().skip(skip_ind).limit(batch_size);
    }
    
    private long getRandomNumber() { 
        long diff = App.COLLECTION_SIZE - this.batch;
        return ThreadLocalRandom.current().nextLong(0, diff);
    }

    public void cleanup() {
        // System.out.println("Going to clean up the states");
        this.mongoConn.close();
        this.mongoConn = null;
        this.database = null;
        this.collection = null;
    }
}