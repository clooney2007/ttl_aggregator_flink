package org.umn.dcs;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LogReplayer {
    public static void main(String[] args) throws IOException, InterruptedException {

        List<String> recordsFileNamesList = Arrays.asList(args[0].split(","));
        double speedUp = Integer.parseInt(args[1]);
        List<String> hosts = Arrays.asList(args[2].split(","));
        int port = Integer.parseInt(args[3]);
        String masterHost = args[4];
        int masterPort = Integer.parseInt(args[5]);
        int edgeId = Integer.parseInt(args[6]);
        System.out.println(String.format("File Names = %s\n", recordsFileNamesList.toString()));
        System.out.println(String.format("Edges = %s Port = %d\n", hosts.toString(), port));
        System.out.println("Sleeping for 100 seconds....");
        Thread.sleep(100 * 1000);
        System.out.println("Sleep complete");
        ExecutorService es = Executors.newFixedThreadPool(recordsFileNamesList.size());
        for(int index=0;index<recordsFileNamesList.size();index++)
            es.execute(new LogReplayerThread(recordsFileNamesList.get(index), hosts.get(index), port, speedUp, masterHost, masterPort, edgeId));
        es.shutdown();
        boolean finshed = es.awaitTermination(1000, TimeUnit.MINUTES);
    }
}
