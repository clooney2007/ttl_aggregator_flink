package org.umn.dcs;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SinkListener {
    private static PrintWriter getWriter(String fileName) throws IOException {
        File file = new File(fileName);
        file.createNewFile();
        FileOutputStream outputFile = new FileOutputStream(file, false);
        return new PrintWriter(outputFile);
    }

    private static class StreamHandler implements Runnable {
        private final Socket clientSock;
        private final String fileName;

        StreamHandler(Socket socket, String fileName) {
            this.clientSock = socket;
            this.fileName = fileName;
        }

        public void run() {
            BufferedReader reader = null;
            String data;
            PrintWriter writer = null;
            try {
                writer = getWriter(this.fileName);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                reader = new BufferedReader(new InputStreamReader(clientSock.getInputStream()));
                while(true) {
                    data = reader.readLine();
                    if (data != null) {
                        writer.println(data);
                        writer.flush();
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (reader != null) reader.close();
                    clientSock.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {

        String sourceHost = args[0];
        int sourcePort = Integer.parseInt(args[1]);

        String recordsFileName = args[2];
        double alpha = Double.parseDouble(args[3]);
        int optimisationType = Integer.parseInt(args[4]);

        int delayCostType = Integer.parseInt(args[5]);
        double delayConstant = Double.parseDouble(args[6]);
        int trafficCostType = Integer.parseInt(args[7]);
        double trafficConstant = Integer.parseInt(args[8]);

        double beta = Double.parseDouble(args[9]);
        double gamma = Double.parseDouble(args[10]);
        double refresh_interval = Double.parseDouble(args[11]);
        double bandwidth = Double.parseDouble(args[12]);
        boolean isCenter = Boolean.parseBoolean(args[13]);

        String centerOutputFileName = Utils.constructCenterOutputFileName(
                recordsFileName,
                optimisationType,
                alpha,
                delayCostType,
                delayConstant,
                trafficCostType,
                trafficConstant,
                beta,
                gamma,
                refresh_interval,
                bandwidth,
                isCenter
        );
        
        try {
            System.out.println("Listening for streams at port " + sourcePort);
            ServerSocket serverSocket = new ServerSocket(sourcePort);
            new StreamHandler(serverSocket.accept(), centerOutputFileName).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
