package com.rishabh.core;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.mongodb.MongoClient;
import com.rishabh.core.ClientThread;

public class App 
{
    public static long COLLECTION_SIZE = 99999;
    public static int RECORD_SIZE = 37;
    public static long READ_IN_UNITS = 1024;
    public static void main( String[] args )
    {

        Scanner sc  = new Scanner(System.in);
        
        System.out.println("Enter the Number of Client Threads to start: ");
        int client_thread = sc.nextInt();
        
        System.out.println("Enter the Batch Size to read the data: ");
        long batch_size = sc.nextInt();

        if(batch_size > COLLECTION_SIZE) {
            batch_size = COLLECTION_SIZE;
        }

        List<ClientThread> myclients = new ArrayList<ClientThread>();

        ClientThread tempClient;
    
        long start = System.currentTimeMillis();
        for (int i = 0; i < client_thread; i++) {
            tempClient = new ClientThread(batch_size, i);
            myclients.add(tempClient);
            tempClient.start();
        }
        for (ClientThread var : myclients) {
            try {
                var.join();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        long dur = end - start;
        // System.out.println("DEBUG: " +String.valueOf(dur) +"ms");
        double thr = calculateThr(dur, batch_size ,client_thread);
        // System.out.println("Throughput A: " + String.valueOf(thr) + " KB/s");
        // System.out.println("Throughput B: " + String.valueOf(thrA) + " KB/s");
        
        writeToFile(args[0], client_thread, batch_size, thr);
        sc.close();
    }

    private static void writeToFile(String path, int client_num, long batch_size, double thrA) {
        FileWriter fileWriter = null;
        File file;
        try{
            file = new File(path);
            fileWriter = new FileWriter(file, true);
            String msg = String.valueOf(client_num) + "," + String.valueOf(batch_size) + "," + String.valueOf(thrA) + "\n";
            fileWriter.write(msg);
            fileWriter.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
    private static double calculateThr(long dur, long batch_size, int client_num) {
        double res = (double) batch_size / (double) READ_IN_UNITS;
        // System.out.println("DEBUG: " + String.valueOf(res));
        res *= (double)client_num;
        res *= (double)((double)RECORD_SIZE / (double)dur);
        // System.out.println("DEBUG: " + String.valueOf(res));
        res *= (double)1000;
        // System.out.println("DEBUG: " + String.valueOf(res));
        return res;
    }
}
