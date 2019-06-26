package org.umn.dcs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GenericRecordParserFlatMap implements FlatMapFunction<String, Record> {
    private final Map<Long, Tuple2<Double, Double>> delayTrafficCostMap;

    Logger LOG = LoggerFactory.getLogger(GenericRecordParserFlatMap.class);

    GenericRecordParserFlatMap(Map<Long, Tuple2<Double, Double>> delayTrafficCostMap) {
        this.delayTrafficCostMap = delayTrafficCostMap;
    }

    @Override
    public void flatMap(String record, Collector<Record> out){
        long currentTimeStamp = System.currentTimeMillis();

        String words[] = record.split(",");
        long key = Long.parseLong(words[0]);

        out.collect(new Record(
                currentTimeStamp,
                key,
                Double.parseDouble(words[1]),
                -1,
                delayTrafficCostMap.get(key).f0,
                delayTrafficCostMap.get(key).f1,
                false
        ));
    }
}