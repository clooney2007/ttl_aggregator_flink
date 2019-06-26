package org.umn.dcs;

import org.slf4j.Logger;

public class KeyArrivalRateState {
    private long key;
    private long firstSeenTimestamp;
    private long lastSeenTimestamp;
    private long numRecordsSeen;
    private long elapsedTime;
    private long TTL;
    private long updateInterval;
    private double unitDelayCost;
    private double unitTrafficCost;
    private double alpha;
    private double arrivalRate;
    private double exp_weight;
    private Logger LOG;
    private long currentUpdateCycle;
    private boolean isDynamic;

    public KeyArrivalRateState() {
    }

    public KeyArrivalRateState(long key, long firstSeenTimestamp, long startTTL, long updateInterval, double unitDelayCost, double unitTrafficCost, double alpha, double exp_weight, Logger log, boolean isDynamic) {
        this.key = key;
        this.firstSeenTimestamp = firstSeenTimestamp;
        this.lastSeenTimestamp = firstSeenTimestamp;
        this.alpha = alpha;
        this.exp_weight = exp_weight;
        this.LOG = log;
        this.isDynamic = isDynamic;
        this.numRecordsSeen = 1;
        this.elapsedTime = 0;
        this.TTL = startTTL;
        this.updateInterval = updateInterval;
        this.unitDelayCost = unitDelayCost;
        this.unitTrafficCost = unitTrafficCost;
        this.arrivalRate = -1;
        this.currentUpdateCycle = 0;
    }

    @Override
    public String toString() {
        return String.format(
                "%d %d %d %d %d %d %d %f %f %f %f %f",
                this.key,
                this.firstSeenTimestamp,
                this.lastSeenTimestamp,
                this.numRecordsSeen,
                this.elapsedTime,
                this.TTL,
                this.updateInterval,
                this.unitDelayCost,
                this.unitTrafficCost,
                this.alpha,
                this.arrivalRate,
                this.exp_weight
        );
    }

    public long getTTL() {
        return this.TTL;
    }
    public long getElapsedTime() {
        return this.elapsedTime;
    }
    public long getNumRecordsSeen() {
        return this.numRecordsSeen;
    }
    public double getArrivalRate() {
        return this.arrivalRate;
    }
    public long getKey() {
        return this.key;
    }

    public void updateState(long currentTimestamp) {
        this.lastSeenTimestamp = currentTimestamp;
        this.numRecordsSeen += 1;
        this.elapsedTime = currentTimestamp - this.firstSeenTimestamp;
        if(this.elapsedTime >= this.updateInterval) {
            recomputeTTL();
        }
    }
    private void recomputeTTL() {
        double currentArrivalRate = (double)this.numRecordsSeen / this.elapsedTime;
//        this.LOG.info(this.toString());
        if(this.arrivalRate == -1)
            this.arrivalRate = currentArrivalRate;
        else
            this.arrivalRate = this.exp_weight * currentArrivalRate + (1-this.exp_weight) * this.arrivalRate;
        if(this.isDynamic) {
            if(alpha == 0.0)
                this.TTL = Constants.MAX_TTL;
            else {
//                double ttl = (Math.sqrt(2.0 * (1 - this.alpha) * this.unitTrafficCost/ (this.alpha * this.unitDelayCost * this.arrivalRate * 1000.0)) - (1/(this.arrivalRate * 1000.0))) * 1000;
                double ttl = 2.0 * (1.0 - this.alpha) * this.unitTrafficCost * this.arrivalRate * 1000 / (this.alpha * this.unitDelayCost);
                if(ttl > 1.0) {
                    ttl = Math.sqrt(ttl - 1.0);
                    ttl = (ttl - 1) / (this.arrivalRate * 1000);
                    ttl = ttl * 1000; // ttl in msec
                }
                else
                    ttl = 0.0;
//            this.LOG.info(String.format("ttl computed = %f", ttl));
                this.TTL = (long) ttl;
                if(this.TTL < 0)
                    this.TTL = 0;
                else if(this.TTL > Constants.MAX_TTL)
                    this.TTL = Constants.MAX_TTL;
            }
//        this.LOG.info(this.toString());
//
//        this.LOG.info(this.toString());
        }
        this.firstSeenTimestamp = this.lastSeenTimestamp;
        this.numRecordsSeen = 1;
        this.elapsedTime = 0;
        this.currentUpdateCycle += 1;
    }

    public long getCurrentUpdateCycle() {
        return currentUpdateCycle;
    }
}
