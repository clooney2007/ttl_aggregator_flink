package org.umn.dcs;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DynamicTTLCacheProcessFunction extends ProcessFunction<Record, EvictedRecord> {

    /**
     * The ValueState handle. The first field is the key, the second field the time  (processing time) to evict.
     * Third field is the list of records which have been aggregated till now.
     */
    private transient ValueState<AggregatedKeyState> keyState;
    private transient ValueState<KeyArrivalRateState> keyArrivalRateState;
    private final int edgeId;
    private final long startTTL;
    private final long ttlUpdateInterval;
    private final double alpha;
    private final double exp_weight;
    private final boolean isDynamic;
    Logger LOG = LoggerFactory.getLogger(DynamicTTLCacheProcessFunction.class);
    private final OutputTag<EvictedRecord> outputTag;

    public DynamicTTLCacheProcessFunction(int edgeId, long startTTL, long ttLupdateInterval, double alpha, double exp_weight, boolean isDynamic, OutputTag<EvictedRecord> outputTag) {
        this.edgeId = edgeId;
        this.startTTL = startTTL;
        ttlUpdateInterval = ttLupdateInterval;
        this.alpha = alpha;
        this.exp_weight = exp_weight;
        this.isDynamic = isDynamic;
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(
            Record input,
            Context ctx,
            Collector<EvictedRecord> out
    ) throws Exception {
        // update the KeyArrivalRateState first
//        LOG.info(String.format("Input %s", input.toString()));
        KeyArrivalRateState currentKeyArrivalRateState = keyArrivalRateState.value();
        if(currentKeyArrivalRateState == null) {
            long ttl = isDynamic ? this.startTTL : input.getTtl();
            currentKeyArrivalRateState = new KeyArrivalRateState(
                input.getKey(),
                input.getArrivalTimestamp(),
                ttl,
                this.ttlUpdateInterval,
                input.getUnitDelayCost(),
                input.getUnitTrafficCost(),
                this.alpha,
                this.exp_weight,
                this.LOG,
                this.isDynamic
            );
            keyArrivalRateState.update(currentKeyArrivalRateState);
        }
        else {
            currentKeyArrivalRateState.updateState(input.getArrivalTimestamp());
            keyArrivalRateState.update(currentKeyArrivalRateState);
        }
//        LOG.info(String.format("Key Arrivate Rate State %s", keyArrivalRateState.value().toString()));
        // access the state value
        AggregatedKeyState currentKeyState = keyState.value();

        // schedule the eviction at T + TTL
        if(currentKeyState == null) {
            long arrivalTime = ctx.timerService().currentProcessingTime();
            long timeForEmit = arrivalTime + currentKeyArrivalRateState.getTTL();
//            LOG.info(String.format("Timer scheduled for %d", timeForEmit));
            List<Record> recordList = new ArrayList<>();
            currentKeyState = new AggregatedKeyState(
                    input.getKey(),
                    arrivalTime,
                    timeForEmit,
                    recordList,
                    currentKeyArrivalRateState.getCurrentUpdateCycle(),
                    currentKeyArrivalRateState.getArrivalRate(),
                    currentKeyArrivalRateState.getTTL()
            );
            currentKeyState.updateRecordList(input);
            keyState.update(currentKeyState);
            ctx.timerService().registerProcessingTimeTimer(timeForEmit);
        }
        else {
            currentKeyState.updateRecordList(input);
            keyState.update(currentKeyState);
        }

//        if(keyState.value() != null)
//            LOG.info(String.format("Key State %s", keyState.value().toString()));
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

            long traffic = 1L;
            double trafficCost = traffic * currentKeyState.getRecordList().get(0).getUnitTrafficCost();
            double recordDelayCost;

            for (Record record : currentKeyState.getRecordList()) {
                recordDelay = currentProcessingTime - record.getArrivalTimestamp();
                delay += recordDelay;
                recordDelayCost= recordDelay * record.getUnitDelayCost();
                delayCost += recordDelayCost;
                value += record.getValue();
                ctx.output(this.outputTag, new EvictedRecord(
                        currentKeyState.getKey(),
                        record.getValue(),
                        recordDelay,
                        recordDelayCost,
                        traffic,
                        trafficCost,
                        record.getArrivalTimestamp(),
                        currentProcessingTime,
                        1,
                        this.edgeId,
                        currentKeyState.getUpdateCycle(),
                        currentKeyState.getArrivalRate(),
                        currentKeyState.getTTL()
                ));
            }


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
        this.keyState = getRuntimeContext().getState(descriptor);

        ValueStateDescriptor<KeyArrivalRateState> keyArrivalRateStateValueStateDescriptor =
                new ValueStateDescriptor<KeyArrivalRateState>(
                        "KeyState Arrival Rate TTLCache",
                        TypeInformation.of(new TypeHint<KeyArrivalRateState>() {}) // type information
                );
        this.keyArrivalRateState = getRuntimeContext().getState(keyArrivalRateStateValueStateDescriptor);
    }
}

