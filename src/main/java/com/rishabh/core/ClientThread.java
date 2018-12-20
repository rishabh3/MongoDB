package com.rishabh.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

public class ClientThread extends Thread {

    private MongoClient mongoConn;
    private MongoDatabase database;
    private MongoCollection<Document> collection;
    private List<Integer> dataRead;
    private Task work;
    private List<Double> throughputDoubles;
    private final Lock listLock = new ReentrantLock();

    private int threadNumber;
    private long batch;

    public ClientThread(long batch, int threadNumber, List<Double> globalThroughpDoubles) {
        this.batch = batch;
        this.threadNumber = threadNumber;
        this.dataRead = new ArrayList<Integer>();
        this.throughputDoubles = globalThroughpDoubles;
    }

    public void run() {
        this.mongoConn = new MongoClient("localhost", 27017);
        this.database = this.mongoConn.getDatabase("test");
        this.collection = this.database.getCollection("mycol");
        long next_task;
        
        long start = System.currentTimeMillis();
        while(work.getStatus() == false) {
            next_task = work.getNextElement();
            if(next_task == -1) {
                continue;
            }
            this.process_cursor(next_task);
        }
        long end = System.currentTimeMillis();
        long duration = end - start;
        int recordRead = this.sumup();
        // System.out.println("Thread " + String.valueOf(this.threadNumber) + " read " + String.valueOf(recordRead));
        
        // Calculate throughput
        double thr = this.calculateThroughput(recordRead, duration);
        // System.out.println("Throughput: " + String.valueOf(thr) + " KB/s");
        // Put the throughput in global structure (Locking)
        listLock.lock();
        try{
            this.throughputDoubles.add(thr);
        }finally{
            listLock.unlock();
        }

        // System.out.println("Connection opened to MongoD instance running at localhost:27017");
        this.cleanup();
    }

    private void process_cursor(long skip) {
        // System.out.println("Read the database skipping " + String.valueOf(skip) +" and limiting " + String.valueOf(this.batch));
        ClientThread clientThread = this;
        int batch_size = (int) clientThread.batch;
        int skip_ind = (int) skip;
        FindIterable<Document> findIterable = clientThread.collection.find().skip(skip_ind).limit(batch_size);
        List<Document> data = new ArrayList<Document>();
        for (Document var : findIterable) {
            data.add(var);
        }
        this.dataRead.add(data.size());
    }
    
    public void start(Task task) {
        // System.out.println("This is start method");
        this.work = task;
        super.start();
    }
    
    public void cleanup() {
        // System.out.println("Going to clean up the states");
        this.mongoConn.close();
        this.mongoConn = null;
        this.database = null;
        this.collection = null;
    }

    private double calculateThroughput(int numRecordsRead, long duration_overall) {
        return (numRecordsRead * App.RECORD_SIZE * 1000) / (App.READ_IN_UNITS * duration_overall);  
    }

    private int sumup() {
        int sum = 0;
        for (Integer var : this.dataRead) {
            sum += var;
        }
        return sum;
    }
}