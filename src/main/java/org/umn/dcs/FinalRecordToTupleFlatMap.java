package org.umn.dcs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinalRecordToTupleFlatMap implements FlatMapFunction<FinalRecord, Tuple6<Long, Double, Long, Double, Long, Double>> {
    Logger LOG = LoggerFactory.getLogger(FinalRecordToTupleFlatMap.class);

    @Override
    public void flatMap(FinalRecord record, Collector<Tuple6<Long, Double, Long, Double, Long, Double>> out){
        out.collect(new Tuple6<>(
                record.getKey(),
                record.getValue(),
                record.getDelay(),
                record.getDelayCost(),
                record.getTraffic(),
                record.getTrafficCost()
        ));
    }
}