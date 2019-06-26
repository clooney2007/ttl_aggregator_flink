package org.umn.dcs;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class KillCenterEdge {
    public static boolean areEqual(Object[] arr1, Object[] arr2)
    {
        int n = arr1.length;
        int m = arr2.length;

        // If lengths of array are not equal means
        // array are not equal
        if (n != m)
            return false;

        // Sort both arrays
        Arrays.sort(arr1);
        Arrays.sort(arr2);

        // Linearly compare elements
        for (int i=0; i<n; i++)
            if (arr1[i] != arr2[i])
                return false;

        // If all elements were same.
        return true;
    }

    public static void main(String[] args) throws IOException {
        int listenerPort = Integer.parseInt(args[0]);
        List<Integer> edgeIds = Arrays
                .stream(args[1].split(","))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        String outputFile = args[2];
        BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFile), StandardOpenOption.APPEND);
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader("EdgeId", "EdgeHost", "ExpectedTime", "ActualTime"));
//        List<Integer> pings_received = Collections.emptyList();
        int pings_received = 0;
        System.out.println(String.format("listing on %s", listenerPort));
        ServerSocket serverSocket = new ServerSocket(listenerPort);
        System.out.println(String.format("Accepted connection on %s", listenerPort));
        try {
//            while (!areEqual(pings_received.toArray(), edgeIds.toArray())) {
            while (pings_received < edgeIds.size()) {

                System.out.println("Waiting for a clinet..");
                Socket socket = serverSocket.accept();
                ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());

                String received = inputStream.readUTF();
                System.out.println(String.format("Received request: %s\n",received));
                List<String> contents = Arrays.asList(received.split(","));
                pings_received = pings_received + 1;
                csvPrinter.printRecord(contents);
                inputStream.close();
                outputStream.close();
                socket.close();
                System.out.println("Closed the current connection..");
            }
            csvPrinter.flush();
            System.out.println("Received requests from all edges. Killing them all! \n");
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Could not listen on port " + listenerPort);
            System.exit(-1);
        }
    }
}
