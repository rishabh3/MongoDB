package com.rishabh.core;
import com.mongodb.MongoClient;

public class MongoConnection {
    private static MongoClient mongoClient = null;

    private MongoConnection() {

    }

    public static MongoClient getConnection(String ip, int port) {
        if(mongoClient == null) {
            mongoClient = new MongoClient(ip, port);
        }
        return mongoClient;
    }
}