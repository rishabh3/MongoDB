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

    public static List<Long> getSkips(long start, long end, long skip) {
        List<Long> mylist = new ArrayList<Long>();
        for( long i = start; i < end;i+=skip ) {
            mylist.add(i);
        }
        if  (mylist.get(mylist.size()-1) < COLLECTION_SIZE) {
            mylist.add(COLLECTION_SIZE);
        }
        return mylist;
    }

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

        List<Long> skips = getSkips(0, COLLECTION_SIZE, batch_size);

        List<Task> thread_work = distributeWork(skips, client_thread, batch_size);

        // for (Task var : thread_work) {
        //     System.out.println(var);   
        // }

        List<ClientThread> myclients = new ArrayList<ClientThread>();

        List<Double> throughput = new ArrayList<Double>();
        ClientThread tempClient;
        int client_num = 0;
        long start = System.currentTimeMillis();
        for (Task var : thread_work) {
            tempClient = new ClientThread(batch_size, client_num, throughput);
            myclients.add(tempClient);
            tempClient.start(var);
            client_num++;
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
        double thr = calculateThr(dur);
        // System.out.println("Throughput A: " + String.valueOf(thr) + " KB/s");
        double sum = 0.0;
        for (Double var : throughput) {
            sum += var;
        }
        double thrA = sum / (double) client_num;
        // System.out.println("Throughput B: " + String.valueOf(thrA) + " KB/s");
        
        writeToFile(args[0], client_num, batch_size, thr, thrA);
        sc.close();
    }

    private static void writeToFile(String path, int client_num, long batch_size, double thrA, double thrB) {
        FileWriter fileWriter = null;
        File file;
        try{
            file = new File(path);
            fileWriter = new FileWriter(file, true);
            String msg = String.valueOf(client_num) + "\t" + String.valueOf(batch_size) + "\t" + String.valueOf(thrA) + "\t" + String.valueOf(thrB)+
            "\n";
            fileWriter.write(msg);
            fileWriter.close();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }
    private static double calculateThr(long dur) {
        double res = (double) COLLECTION_SIZE / (double) READ_IN_UNITS;
        // System.out.println("DEBUG: " + String.valueOf(res));
        res *= (double)((double)RECORD_SIZE / (double)dur);
        // System.out.println("DEBUG: " + String.valueOf(res));
        res *= (double)1000;
        // System.out.println("DEBUG: " + String.valueOf(res));
        return res;
    }

    private static List<Task> distributeWork(List<Long> skips, int thread_count, long batch_size) {
        List<Task> tasks = new ArrayList<Task>();
        int start = 0, end = 0;
        int num_groups = (int) (skips.size() / thread_count);
        Task tempTask;
        for(int i = 0;i < thread_count;i++ ){
            if (i == thread_count - 1) {
                tempTask = new Task(skips, end, skips.size()-1, i, batch_size);
                tasks.add(tempTask);
            }
            else {
                start = i* num_groups;
                end = (i+1) * num_groups;
                tempTask = new Task(skips, start, end, i, batch_size);
                tasks.add(tempTask);
            }
        }
        return tasks;
    }
}
