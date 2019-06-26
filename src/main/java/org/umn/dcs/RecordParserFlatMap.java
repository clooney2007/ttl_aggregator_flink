package org.umn.dcs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RecordParserFlatMap implements FlatMapFunction<String, Record> {
    private final Map<Long, Long> ttlMap;
    private final Map<Long, Tuple2<Double, Double>> delayTrafficCostMap;

    Logger LOG = LoggerFactory.getLogger(RecordParserFlatMap.class);

    RecordParserFlatMap(Map<Long, Long> ttlMap, Map<Long, Tuple2<Double, Double>> delayTrafficCostMap) {
        this.ttlMap = ttlMap;
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
                ttlMap.get(key),
                delayTrafficCostMap.get(key).f0,
                delayTrafficCostMap.get(key).f1,
                Integer.parseInt(words[2]) == 1
        ));
    }
}