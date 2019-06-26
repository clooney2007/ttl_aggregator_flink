package org.umn.dcs;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TTLCacheProcessFunction extends ProcessFunction<Record, EvictedRecord> {

    /**
     * The ValueState handle. The first field is the key, the second field the time  (processing time) to evict.
     * Third field is the list of records which have been aggregated till now.
     */
    private transient ValueState<AggregatedKeyState> keyState;
    private final int edgeId;
    Logger LOG = LoggerFactory.getLogger(TTLCacheProcessFunction.class);

    public TTLCacheProcessFunction(int edgeId) {
        this.edgeId = edgeId;
    }

    @Override
    public void processElement(
            Record input,
            Context ctx,
            Collector<EvictedRecord> out
    ) throws Exception {

        // access the state value
        AggregatedKeyState currentKeyState = keyState.value();

        // schedule the eviction at T + TTL
        if(currentKeyState == null) {
            long arrivalTime = ctx.timerService().currentProcessingTime();
            long timeForEmit = arrivalTime + input.getTtl();
            List<Record> recordList = new ArrayList<>();
            currentKeyState = new AggregatedKeyState(
                    input.getKey(),
                    arrivalTime,
                    timeForEmit,
                    recordList,
                    -1,
                    0.0,
                    1
            );
            currentKeyState.updateRecordList(input);
            keyState.update(currentKeyState);
            ctx.timerService().registerProcessingTimeTimer(timeForEmit);
        }
        else {
            currentKeyState.updateRecordList(input);
            keyState.update(currentKeyState);
        }

        if(input.isLastOccurence()) {
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EvictedRecord> out)
            throws Exception {
        // get the state for the key that scheduled the timer
        AggregatedKeyState currentKeyState = keyState.value();

        if(currentKeyState != null) {

            long delay = 0L, recordDelay;
            double delayCost = 0;
            double value = 0;
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            for (Record record : currentKeyState.getRecordList()) {
                recordDelay = currentProcessingTime - record.getArrivalTimestamp();
                delay += recordDelay;
                delayCost += recordDelay * record.getUnitDelayCost();
                value += record.getValue();
            }

            long traffic = 1L;
            double trafficCost = traffic * currentKeyState.getRecordList().get(0).getUnitTrafficCost();
            out.collect(new EvictedRecord(
                    currentKeyState.getKey(),
                    value,
                    delay,
                    delayCost,
                    traffic,
                    trafficCost,
                    currentKeyState.getArrivalTimestamp(),
                    currentProcessingTime,
                    currentKeyState.getRecordList().size(),
                    this.edgeId,
                    currentKeyState.getUpdateCycle(),
                    currentKeyState.getArrivalRate(),
                    currentKeyState.getTTL()
            ));
            keyState.update(null);
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<AggregatedKeyState> descriptor =
                new ValueStateDescriptor<AggregatedKeyState>(
                        "TTLCache",
                        TypeInformation.of(new TypeHint<AggregatedKeyState>() {}) // type information
                );
        keyState = getRuntimeContext().getState(descriptor);
    }
}

