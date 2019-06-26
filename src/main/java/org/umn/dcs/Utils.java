package org.umn.dcs;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {
    public static String constructTTLFileName(
            String recordsFileName,
            int optimisationType,
            double alpha,
            int delayCostType,
            double delayConstant,
            int trafficCostType,
            double trafficConstant,
            double beta,
            double gamma,
            double refresh_interval
    ) {
        return String.format(
                "%s_ttl_opt_%d_p_%s_d_%d_%s_c_%d_%s_b_%s_g_%s_r_%s.csv",
                recordsFileName.split(".csv")[0],
                optimisationType,
                formatFloat(alpha),
                delayCostType,
                formatFloat(delayConstant),
                trafficCostType,
                formatFloat(trafficConstant),
                formatFloat(beta),
                formatFloat(gamma),
                formatFloat(refresh_interval)
            );
    }

    public static String constructCenterOutputFileName(
            String recordsFileName,
            int optimisationType,
            double alpha,
            int delayCostType,
            double delayConstant,
            int trafficCostType,
            double trafficConstant,
            double beta,
            double gamma,
            double refresh_interval,
            double bandwidth,
            boolean isCenter
    ) {
        if(isCenter)
            return String.format(
                    "%s_center_output_opt_%d_p_%s_d_%d_%s_c_%d_%s_b_%s_g_%s_r_%s_bw_%s.csv",
                    recordsFileName.split(".csv")[0],
                    optimisationType,
                    formatFloat(alpha),
                    delayCostType,
                    formatFloat(delayConstant),
                    trafficCostType,
                    formatFloat(trafficConstant),
                    formatFloat(beta),
                    formatFloat(gamma),
                    formatFloat(refresh_interval),
                    formatFloat(bandwidth)
            );
        else
            return String.format(
                    "%s_edge_output_opt_%d_p_%s_d_%d_%s_c_%d_%s_b_%s_g_%s_r_%s_bw_%s.csv",
                    recordsFileName.split(".csv")[0],
                    optimisationType,
                    formatFloat(alpha),
                    delayCostType,
                    formatFloat(delayConstant),
                    trafficCostType,
                    formatFloat(trafficConstant),
                    formatFloat(beta),
                    formatFloat(gamma),
                    formatFloat(refresh_interval),
                    formatFloat(bandwidth)
            );
    }

    public static String constructDelayTrafficCostFileName(
            String recordsFileName,
            int delayCostType,
            double delayConstant,
            int trafficCostType,
            double trafficConstant,
            double beta,
            double gamma
    ) {
        return String.format(
                "%s_delay_traffic_costs_d_%d_%s_c_%d_%s_b_%s_g_%s.csv",
                recordsFileName.split(".csv")[0],
                delayCostType,
                formatFloat(delayConstant),
                trafficCostType,
                formatFloat(trafficConstant),
                formatFloat(beta),
                formatFloat(gamma)
        );
    }

    public static Map<Long, Long> readTTLCSV(
            String recordsFileName,
            int optimisationType,
            double alpha,
            int delayCostType,
            double delayConstant,
            int trafficCostType,
            double trafficConstant,
            double beta,
            double gamma,
            double refresh_interval
    ) throws IOException {
        String fileName = constructTTLFileName(
                recordsFileName,
                optimisationType,
                alpha,
                delayCostType,
                delayConstant,
                trafficCostType,
                trafficConstant,
                beta,
                gamma,
                refresh_interval
        );

        Reader fileReader = new FileReader(fileName);
        CSVParser parser = new CSVParser(fileReader, CSVFormat.RFC4180.withFirstRecordAsHeader());
        List<CSVRecord> list = parser.getRecords();

        return list.stream().collect(Collectors.toMap(
                record -> Long.parseLong(record.get("key")),
                record -> (long)(Double.parseDouble(record.get("ttl")) * 1000) // convert to milliseconds
        ));
    }

    public static Map<Long, Tuple2<Double, Double>> readDelayTrafficCostCSV(
            String recordsFileName,
            int delayCostType,
            double delayConstant,
            int trafficCostType,
            double trafficConstant,
            double beta,
            double gamma
    ) throws IOException {
        String fileName = constructDelayTrafficCostFileName(
                recordsFileName,
                delayCostType,
                delayConstant,
                trafficCostType,
                trafficConstant,
                beta,
                gamma
        );

        Reader fileReader = new FileReader(fileName);
        CSVParser parser = new CSVParser(fileReader, CSVFormat.RFC4180.withFirstRecordAsHeader());
        List<CSVRecord> list = parser.getRecords();

        return list.stream().collect(Collectors.toMap(
                record -> Long.parseLong(record.get("key")),
                record -> new Tuple2<Double, Double>(
                        Double.parseDouble(record.get("delay_cost")),
                        Double.parseDouble(record.get("traffic_cost"))
                )
        ));
    }

    public static String formatFloat(double value) {
        return String.format("%.8f", value);
    }

    public static long computeMeanTTL(Map<Long, Long> ttlMap) {
        Iterator it = ttlMap.entrySet().iterator();
        long mean = 0L;
        while (it.hasNext()) {
            mean += (long)((Map.Entry)it.next()).getValue();
        }
        return mean / ttlMap.size();
    }

    public static List<Tuple2<Long, String>> readRecords(String recordsFileName) throws IOException {
        System.out.println("Constructing the max timestamp dictionary...");
        Map<String, Long> maxTimestampDict = constructMaxTimestampDict(recordsFileName);

        System.out.println(String.format("Reading %s.....", recordsFileName));
        Reader fileReader = new FileReader(recordsFileName);
        CSVParser parser = new CSVParser(fileReader, CSVFormat.RFC4180.withFirstRecordAsHeader());

        System.out.println("Converting to list...");
        List<Tuple2<Long, String>> recordList = new ArrayList<>();

        String key, value;
        long timestamp;
        int lastOccurence;
        for (CSVRecord record: parser) {
            key = record.get("key").trim();
            value = record.get("value").trim();
            timestamp = (long)(Double.parseDouble(record.get("timestamp")) * 1e9);
            lastOccurence = (timestamp == maxTimestampDict.get(key)) ? 1 : 0;
            recordList.add(new Tuple2<>(
                    Double.valueOf(Double.parseDouble(record.get("timestamp")) * 1e9).longValue(),
                    String.format("%s,%s,%s\n", key, value, lastOccurence)
            ));
        }
        System.out.println("Converted to list...");
        return recordList;
    }

    public static Map<String,Long> constructMaxTimestampDict(String fileName) throws IOException {
        Reader fileReader = new FileReader(fileName);
        CSVParser parser = new CSVParser(fileReader, CSVFormat.RFC4180.withFirstRecordAsHeader());
        Map<String, Long> maxTimestampDict = new HashMap<>();
        String key;
        long timestamp;
        for (CSVRecord record: parser) {
            key = record.get("key").trim();
            timestamp = (long)(Double.parseDouble(record.get("timestamp")) * 1e9);
            maxTimestampDict.put(key, timestamp);
        }
        fileReader.close();
        parser.close();
        return maxTimestampDict;
    }

    public static void busyWaitNanos(long nanos){
        long waitUntil = System.nanoTime() + nanos;
        while(waitUntil > System.nanoTime());
    }

}
