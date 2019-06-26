package org.umn.dcs;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PerKeyTumblingWindowProcessFunction extends ProcessFunction<Record, EvictedRecord> {

    /**
     * The ValueState handle. The first field is the key, the second field the time  (processing time) to evict.
     * Third field is the list of records which have been aggregated till now.
     */
    private transient ValueState<AggregatedKeyState> keyState;
    private final int edgeId;
    Logger LOG = LoggerFactory.getLogger(PerKeyTumblingWindowProcessFunction.class);
    private final OutputTag<EvictedRecord> outputTag;


    public PerKeyTumblingWindowProcessFunction(int edgeId, OutputTag<EvictedRecord> outputTag) {
        this.edgeId = edgeId;
        this.outputTag = outputTag;
    }

    @Override
    public void processElement(
            Record input,
            Context ctx,
            Collector<EvictedRecord> out
    ) throws Exception {

        // access the state value
        AggregatedKeyState currentKeyState = keyState.value();

        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        long windowStartTime, windowEndTime;
        if(input.getTtl() > 0) {
            windowStartTime = TimeWindow.getWindowStartWithOffset(currentProcessingTime, 0, input.getTtl());
            windowEndTime = windowStartTime + input.getTtl();
        }
        else {
            windowEndTime = currentProcessingTime;
        }


        // schedule the eviction at window_start_time + TTL
        if(currentKeyState == null) {
            List<Record> recordList = new ArrayList<>();
            currentKeyState = new AggregatedKeyState(
                    input.getKey(),
                    currentProcessingTime,
                    windowEndTime,
                    recordList,
                    -1,
                    -1.0,
                    -1
            );
            currentKeyState.updateRecordList(input);
            keyState.update(currentKeyState);
        }
        else {
            currentKeyState.updateRecordList(input);
            keyState.update(currentKeyState);
        }

        // schedule eviction at windowEndTime
        ctx.timerService().registerProcessingTimeTimer(windowEndTime);

        if(input.isLastOccurence()) {
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime);
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
            double recordDelayCost;
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            long traffic = 1L;
            double trafficCost = traffic * currentKeyState.getRecordList().get(0).getUnitTrafficCost();

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
                        "PerKeyTumblingWindow",
                        TypeInformation.of(new TypeHint<AggregatedKeyState>() {}) // type information
                );
        keyState = getRuntimeContext().getState(descriptor);
    }
}

