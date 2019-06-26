package org.umn.dcs;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LogReplayerThread implements Runnable {
    private final String fileName;
    private final String host;
    private final int port;
    private final double speedUp;
    private final String masterHost;
    private final int masterPort;
    private final int edgeId;

    LogReplayerThread(String fileName, String host, int port, double speedUp, String masterHost, int masterPort, int edgeId) {
        this.fileName = fileName;
        this.host = host;
        this.port = port;
        this.speedUp = speedUp;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.edgeId = edgeId;
    }

    @Override
    public void run() {
        List<Tuple2<Long, String>> recordList = null;
        try {
            recordList = Utils.readRecords(this.fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(recordList.size() == 0)
            return;

        Socket socket = null;
        try {
            socket = new Socket(this.host, this.port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        PrintWriter out = null;
        try {
            out = new PrintWriter(socket.getOutputStream(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        long totalDelta = (long)((recordList.get(recordList.size()-1).f0 - recordList.get(0).f0) / this.speedUp);
        System.out.println(String.format("Sending %s requests to %s:%s speedup = %s total_delta =%s seconds", recordList.size(), this.host, this.port, this.speedUp, TimeUnit.SECONDS.convert(totalDelta, TimeUnit.NANOSECONDS)));

        long lastTimestamp = recordList.get(0).f0;
        long timeDelta;
        long requestsSent = 0;
        long currentTime = System.nanoTime();
        for (int i = 0; i < recordList.size(); i++) {
            timeDelta = (long) ((recordList.get(i).f0 - lastTimestamp) / this.speedUp);
            Utils.busyWaitNanos(timeDelta);
            out.print(recordList.get(i).f1);
            out.flush();
            lastTimestamp = recordList.get(i).f0;
//            System.out.println(recordList.get(i).f1);
//            requestsSent++;
//            if (requestsSent % 10000 == 0)
//                System.out.println(String.format("Number of requests sent = %s", requestsSent));
        }
        long actualDelta = System.nanoTime() - currentTime;
        System.out.println(String.format("Host: %s Time taken: %s seconds Expected time taken: %s seconds", this.host,
                TimeUnit.SECONDS.convert(actualDelta, TimeUnit.NANOSECONDS),
                TimeUnit.SECONDS.convert(totalDelta, TimeUnit.NANOSECONDS)
        ));
        System.out.println(String.format("Number of requests sent = %s",requestsSent));

//        Thread.sleep(50000);
//        out.println("END");
        out.close();
        try {
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Sleeping for 20 seconds before sending the kill request..");
        try {
            Thread.sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Sleep over\n. Sending kill request to = %s %s",this.masterHost, this.masterPort));
        try {
            String message = String.format(
                    "%s,%s,%s,%s\n",
                    this.edgeId,
                    this.host,
                    TimeUnit.SECONDS.convert(totalDelta, TimeUnit.NANOSECONDS),
                    TimeUnit.SECONDS.convert(actualDelta, TimeUnit.NANOSECONDS)
            );
            Socket masterSocket = new Socket(this.masterHost, this.masterPort);
            ObjectOutputStream outputStream = new ObjectOutputStream(masterSocket.getOutputStream());
            outputStream.writeUTF(message);
            outputStream.flush();
            outputStream.close();
//            PrintWriter outMaster = new PrintWriter(masterSocket.getOutputStream(), true);
//            outMaster.print(message);
//            Thread.sleep(15000);
//            outMaster.close();
            masterSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Sent kill request to = %s %s",this.masterHost, this.masterPort));
    }
}