package org.umn.dcs;

public class FinalRecord {
    private long key;
    private double value;
    private long delay;
    private double delayCost;
    private long traffic;
    private double trafficCost;
    private long updateCycle;
    private double arrivalRate;
    private long TTL;
    public FinalRecord() {
    }

    public FinalRecord(long key, double value, long delay, double delayCost, long traffic, double trafficCost, long updateCycle, double arrivalRate, long ttl) {
        this.key = key;
        this.value = value;
        this.delay = delay;
        this.delayCost = delayCost;
        this.traffic = traffic;
        this.trafficCost = trafficCost;
        this.updateCycle = updateCycle;
        this.arrivalRate = arrivalRate;
        this.TTL = ttl;
    }

    public long getKey() {
        return this.key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public long getDelay() {
        return this.delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public long getTraffic() {
        return this.traffic;
    }

    public void setTraffic(long traffic) {
        this.traffic = traffic;
    }

    public double getDelayCost() {
        return this.delayCost;
    }

    public void setDelayCost(double delayCost) {
        this.delayCost = delayCost;
    }

    public double getTrafficCost() {
        return this.trafficCost;
    }

    public void setTrafficCost(double trafficCost) {
        this.trafficCost = trafficCost;
    }


    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format(
                "%d,%f,%d,%f,%d,%f,%d,%f,%d",
                this.key,
                this.value,
                this.delay,
                this.delayCost,
                this.traffic,
                this.trafficCost,
                this.updateCycle,
                this.arrivalRate,
                this.TTL
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
}
