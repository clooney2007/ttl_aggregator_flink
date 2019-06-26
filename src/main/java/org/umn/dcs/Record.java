package org.umn.dcs;

public class Record {
    private long arrivalTimestamp; // timestamp assigned as soon as the record enters the Flink system
//    private long sourceTimestamp; // timestamp assigned by the source (comes with the record itself)
    private long key;
    private double value;
    private long ttl; // in milliseconds
    private double unitDelayCost;
    private double unitTrafficCost;
    private boolean lastOccurence;

    public Record(){}

    public Record(long arrivalTimestamp, long key, double value, long ttl, double unitDelayCost, double unitTrafficCost, boolean lastOccurence) {
        this.arrivalTimestamp = arrivalTimestamp;
        this.key = key;
        this.value = value;
        this.ttl = ttl;
        this.unitDelayCost = unitDelayCost;
        this.unitTrafficCost = unitTrafficCost;
        this.lastOccurence = lastOccurence;
    }


    public long getArrivalTimestamp() {
        return this.arrivalTimestamp;
    }

    public void setArrivalTimestamp(long arrivalTimestamp) {
        this.arrivalTimestamp = arrivalTimestamp;
    }

    public long getKey() {
        return this.key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public double getValue() {
        return this.value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public long getTtl() {
        return this.ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public double getUnitDelayCost() {
        return this.unitDelayCost;
    }

    public void setUnitDelayCost(double unitDelayCost) {
        this.unitDelayCost = unitDelayCost;
    }

    public double getUnitTrafficCost() {
        return this.unitTrafficCost;
    }

    public void setUnitTrafficCost(double unitTrafficCost) {
        this.unitTrafficCost = unitTrafficCost;
    }

    public boolean isLastOccurence() {
        return this.lastOccurence;
    }

    public void setLastOccurence(boolean lastOccurence) {
        this.lastOccurence = lastOccurence;
    }

    @Override
    public String toString() {
        return String.format(
                "%d %d %f %d %f %f %b",
                this.arrivalTimestamp,
                this.key,
                this.value,
                this.ttl,
                this.unitDelayCost,
                this.unitTrafficCost,
                this.lastOccurence
        );
    }

}
