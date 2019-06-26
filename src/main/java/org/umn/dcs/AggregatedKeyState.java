package org.umn.dcs;

import java.util.List;

public class AggregatedKeyState {
    private long key;
    private long arrivalTimestamp;
    private long evictionTimestamp;
    private List<Record> recordList;
    private long updateCycle;
    private double arrivalRate;
    private long TTL;
    public AggregatedKeyState() {
    }

    public AggregatedKeyState(long key, long arrivalTimestamp, long evictionTimestamp, List<Record> recordList, long updateCycle, double arrivalRate, long ttl) {
        this.key = key;
        this.arrivalTimestamp = arrivalTimestamp;
        this.evictionTimestamp = evictionTimestamp;
        this.recordList = recordList;
        this.updateCycle = updateCycle;
        this.arrivalRate = arrivalRate;
        TTL = ttl;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getEvictionTimestamp() {
        return evictionTimestamp;
    }

    public void setEvictionTimestamp(long evictionTimestamp) {
        this.evictionTimestamp = evictionTimestamp;
    }

    public List<Record> getRecordList() {
        return recordList;
    }

    public void setRecordList(List<Record> recordList) {
        this.recordList = recordList;
    }

    public void updateRecordList(Record record) {
        this.recordList.add(record);
    }

    @Override
    public String toString() {
        return String.format(
                "%d %d %d",
                this.key,
                this.evictionTimestamp,
                this.recordList.size()
        );
    }

    public long getUpdateCycle() {
        return updateCycle;
    }

    public double getArrivalRate() {
        return arrivalRate;
    }

    public long getTTL() {
        return TTL;
    }

    public long getArrivalTimestamp() {
        return arrivalTimestamp;
    }
}
