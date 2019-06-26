package org.umn.dcs;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class EvictedRecord {
    private FinalRecord aggregatedRecord;
    private String localIpAddress;
    private int edgeId;
    private long departureTimestamp;
    private long arrivalTimestamp;
    private int numRecordsAggregated;
    public EvictedRecord() {

    }
    public EvictedRecord(String record) {
        String words[] = record.split(",");
        this.localIpAddress = words[0];
        this.edgeId = Integer.parseInt(words[1]);
        this.arrivalTimestamp = Long.parseLong(words[2]);
        this.departureTimestamp = Long.parseLong(words[3]);
        this.numRecordsAggregated = Integer.parseInt(words[4]);
        this.aggregatedRecord = new FinalRecord(
                Long.parseLong(words[5]),
                Double.parseDouble(words[6]),
                Long.parseLong(words[7]),
                Double.parseDouble(words[8]),
                Long.parseLong(words[9]),
                Double.parseDouble(words[10]),
                Long.parseLong(words[11]),
                Double.parseDouble(words[12]),
                Long.parseLong(words[13])
        );
    }

    public EvictedRecord(long key, double value, long delay, double delayCost, long traffic, double trafficCost, long arrivalTimestamp, long departureTimestamp, int numRecordsAggregated, int edgeId, long updateCycle, double arrivalRate, long TTL) throws UnknownHostException {
        this.aggregatedRecord = new FinalRecord(
                key,
                value,
                delay,
                delayCost,
                traffic,
                trafficCost,
                updateCycle,
                arrivalRate,
                TTL
        );
        this.localIpAddress = InetAddress.getLocalHost().getHostAddress();
        this.edgeId = edgeId;
        this.departureTimestamp = departureTimestamp;
        this.arrivalTimestamp = arrivalTimestamp;
        this.numRecordsAggregated = numRecordsAggregated;
    }

    @Override
    public String toString() {
        return String.format(
                "%s,%d,%d,%d,%d,%s",
                this.localIpAddress,
                this.edgeId,
                this.arrivalTimestamp,
                this.departureTimestamp,
                this.numRecordsAggregated,
                this.aggregatedRecord.toString()
        );
    }


    public String getLocalIpAddress() {
        return localIpAddress;
    }

    public void setLocalIpAddress(String localIpAddress) {
        this.localIpAddress = localIpAddress;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public void setEdgeId(int edgeId) {
        this.edgeId = edgeId;
    }

    public int getNumRecordsAggregated() {
        return numRecordsAggregated;
    }

    public void setNumRecordsAggregated(int numRecordsAggregated) {
        this.numRecordsAggregated = numRecordsAggregated;
    }

    public long getDepartureTimestamp() {
        return departureTimestamp;
    }

    public long getArrivalTimestamp() {
        return arrivalTimestamp;
    }

    public void setDepartureTimestamp(long departureTimestamp) {
        this.departureTimestamp = departureTimestamp;
    }

    public FinalRecord getAggregatedRecord() {
        return aggregatedRecord;
    }

    public void setAggregatedRecord(FinalRecord aggregatedRecord) {
        this.aggregatedRecord = aggregatedRecord;
    }
}
