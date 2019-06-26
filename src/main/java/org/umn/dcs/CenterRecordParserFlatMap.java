package org.umn.dcs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CenterRecordParserFlatMap implements FlatMapFunction<String, String> {

    Logger LOG = LoggerFactory.getLogger(CenterRecordParserFlatMap.class);

    @Override
    public void flatMap(String record, Collector<String> out){
        long currentTimeStamp = System.currentTimeMillis();
        EvictedRecord evictedRecordFromEdge = new EvictedRecord(record);
        long congestionDelay = (currentTimeStamp - evictedRecordFromEdge.getDepartureTimestamp()) * evictedRecordFromEdge.getNumRecordsAggregated();
        double unitDelayCost = evictedRecordFromEdge.getAggregatedRecord().getDelayCost() / evictedRecordFromEdge.getAggregatedRecord().getDelay();
        double congestionDelayCost = congestionDelay * unitDelayCost;
        long updatedDelay = congestionDelay + evictedRecordFromEdge.getAggregatedRecord().getDelay();

        double updatedDelayCost = updatedDelay * unitDelayCost;
        out.collect(String.format(
                "%s,%d,%d,%d,%d,%d,%f,%d,%d,%f,%d,%f,%d,%f,%d,%f,%d,%f,%d\n",
                evictedRecordFromEdge.getLocalIpAddress(),
                evictedRecordFromEdge.getEdgeId(),
                currentTimeStamp,
                evictedRecordFromEdge.getArrivalTimestamp(),
                evictedRecordFromEdge.getDepartureTimestamp(),
                evictedRecordFromEdge.getAggregatedRecord().getKey(),
                evictedRecordFromEdge.getAggregatedRecord().getValue(),
                evictedRecordFromEdge.getNumRecordsAggregated(),
                updatedDelay,
                updatedDelayCost,
                evictedRecordFromEdge.getAggregatedRecord().getDelay(),
                evictedRecordFromEdge.getAggregatedRecord().getDelayCost(),
                congestionDelay,
                congestionDelayCost,
                evictedRecordFromEdge.getAggregatedRecord().getTraffic(),
                evictedRecordFromEdge.getAggregatedRecord().getTrafficCost(),
                evictedRecordFromEdge.getAggregatedRecord().getUpdateCycle(),
                evictedRecordFromEdge.getAggregatedRecord().getArrivalRate(),
                evictedRecordFromEdge.getAggregatedRecord().getTTL()
        ));
    }
}
