package org.umn.dcs;

public class AggregateMetrics {
    private long key;
    private double value;
    private long sumTimestamps;
    private int numRecordsAggregated;
    private double delayConstant;
    private double trafficConstant;
    public AggregateMetrics() {
    }

    public AggregateMetrics(long key, double value, long sumTimestamps, int numRecordsAggregated, double delayConstant, double trafficConstant) {
        this.key = key;
        this.value = value;
        this.sumTimestamps = sumTimestamps;
        this.numRecordsAggregated = numRecordsAggregated;
        this.delayConstant = delayConstant;
        this.trafficConstant = trafficConstant;
    }

    public long getKey() {
        return this.key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getSumTimestamps() {
        return this.sumTimestamps;
    }

    public void setSumTimestamps(long sumTimestamps) {
        this.sumTimestamps = sumTimestamps;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public int getNumRecordsAggregated() {
        return numRecordsAggregated;
    }

    public void setNumRecordsAggregated(int numRecordsAggregated) {
        this.numRecordsAggregated = numRecordsAggregated;
    }

    @Override
    public String toString() {
        return String.format(
                "%d,%f,%d,%d",
                this.key,
                this.value,
                this.sumTimestamps,
                this.numRecordsAggregated
        );
    }

    public double getDelayConstant() {
        return delayConstant;
    }

    public void setDelayConstant(double delayConstant) {
        this.delayConstant = delayConstant;
    }

    public double getTrafficConstant() {
        return trafficConstant;
    }

    public void setTrafficConstant(double trafficConstant) {
        this.trafficConstant = trafficConstant;
    }
}
