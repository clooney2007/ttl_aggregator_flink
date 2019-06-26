package org.umn.dcs;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TTLCacheJobEdge {
	static Logger LOG = LoggerFactory.getLogger(TTLCacheJobEdge.class);

	public static void main(String[] args) throws Exception {
		String sourceHost = args[0];
		int sourcePort = Integer.parseInt(args[1]);

		String recordsFileName = args[2];
		double alpha = Double.parseDouble(args[3]);
		int optimisationType = Integer.parseInt(args[4]);

		int delayCostType = Integer.parseInt(args[5]);
		double delayConstant = Double.parseDouble(args[6]);
		int trafficCostType = Integer.parseInt(args[7]);
		double trafficConstant = Double.parseDouble(args[8]);

        double beta = Double.parseDouble(args[9]);
		double gamma = Double.parseDouble(args[10]);
		double refresh_interval = Double.parseDouble(args[11]);

		String destinationHost = args[12];
		int destinationPort = Integer.parseInt(args[13]);

		int edgeId = Integer.parseInt(args[14]);

		boolean dynamic = Boolean.parseBoolean(args[15]);
		long startTTL = Long.parseLong(args[16]) * 1000; // converting from sec to millisec
		long ttlUpdateInterval = Long.parseLong(args[17]) * 1000; // converting from sec to millisec
		double exp_weight = Double.parseDouble(args[18]);
		String ttlRecordsFileName = args[19];
		System.out.println(String.format("Edge %d\n", edgeId));

		// read the TTL data and store it in memory.
		Map<Long, Long> ttlMap = Utils.readTTLCSV(
				ttlRecordsFileName,
				optimisationType,
				alpha,
				delayCostType,
				delayConstant,
				trafficCostType,
				trafficConstant,
                beta,
                gamma,
				refresh_interval
		);

		// read the delay and traffic cost and store it in memory.
		Map<Long, Tuple2<Double, Double>> delayTrafficCostMap = Utils.readDelayTrafficCostCSV(
				ttlRecordsFileName,
				delayCostType,
				delayConstant,
				trafficCostType,
				trafficConstant,
                beta,
                gamma
		);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        SingleOutputStreamOperator<EvictedRecord> dataStream;
		// add data source
//		DataStream<EvictedRecord> dataStream = env
//				.addSource(new CustomSocketTextStreamFunction(sourceHost, sourcePort, "\n", 0))
//				.flatMap(new RecordParserFlatMap(ttlMap, delayTrafficCostMap))
//				.keyBy(new KeySelector<Record, Long>() {
//					public Long getKey(Record record) { return record.getKey(); }
//				})
//				.process(new TTLCacheProcessFunction(edgeId));

        KeyedStream<Record, Long> keyedStream = env
                .addSource(new CustomSocketTextStreamFunction(sourceHost, sourcePort, "\n", 0))
                .flatMap(new RecordParserFlatMap(ttlMap, delayTrafficCostMap))
                .keyBy(new KeySelector<Record, Long>() {
                    public Long getKey(Record record) { return record.getKey(); }
                });

		OutputTag<EvictedRecord> outputTag = new OutputTag<EvictedRecord>("edge-output") {};
        List<Integer> ttlCacheOptTypes = Arrays.asList(1, 2, 3, 11, 17, 23, 24);
        if(ttlCacheOptTypes.contains(optimisationType)) {
        	LOG.info("running ttl cache flink....");
//        	if(dynamic) {
//				System.out.println("Going dynamic....\n");
				dataStream = keyedStream.process(new DynamicTTLCacheProcessFunction(edgeId, startTTL, ttlUpdateInterval, alpha, exp_weight, dynamic, outputTag));
//			}
//			else
//            	dataStream = keyedStream.process(new TTLCacheProcessFunction(edgeId));
        }
        else {
            dataStream = keyedStream.process(new PerKeyTumblingWindowProcessFunction(edgeId, outputTag));
        }

		dataStream.writeToSocket(destinationHost, destinationPort, new SerializationSchema<EvictedRecord>() {
			Logger LOG = LoggerFactory.getLogger(SerializationSchema.class);
			@Override
			public byte[] serialize(EvictedRecord record) {
				String result = String.format("%s\n", record.toString());
//				LOG.info("Sending {}", result);
				return result.getBytes();
			}
		});

		DataStream<EvictedRecord> sideOutputStream = dataStream.getSideOutput(outputTag);
		sideOutputStream.writeToSocket(sourceHost, destinationPort, new SerializationSchema<EvictedRecord>() {
			@Override
			public byte[] serialize(EvictedRecord record) {
				String result = String.format("%s\n", record.toString());
				return result.getBytes();
			}
		});

		// execute program
		env.execute("TTL Cache Edge");
	}
}
