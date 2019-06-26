package org.umn.dcs;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TumblingWindowEdge {
	Logger LOG = LoggerFactory.getLogger(TumblingWindowEdge.class);

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


		// read the delay and traffic cost and store it in memory.
		Map<Long, Tuple2<Double, Double>> delayTrafficCostMap = Utils.readDelayTrafficCostCSV(
				recordsFileName,
				delayCostType,
				delayConstant,
				trafficCostType,
				trafficConstant,
				beta,
				gamma
		);

		// read the TTL data and store it in memory.
		Map<Long, Long> ttlMap = Utils.readTTLCSV(
				recordsFileName,
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

		long windowSize = Utils.computeMeanTTL(ttlMap);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// add data source
		DataStream<EvictedRecord> dataStream = env
				.addSource(new CustomSocketTextStreamFunction(sourceHost, sourcePort, "\n", 0))
				.flatMap(new GenericRecordParserFlatMap(delayTrafficCostMap))
				.keyBy(new KeySelector<Record, Long>() {
					public Long getKey(Record record) { return record.getKey(); }
				})
				.window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowSize)))
				.aggregate(new MetricAggergate(), new MyProcessWindowFunction(edgeId));


		dataStream.writeToSocket(destinationHost, destinationPort, new SerializationSchema<EvictedRecord>() {
			Logger LOG = LoggerFactory.getLogger(SerializationSchema.class);
			@Override
			public byte[] serialize(EvictedRecord record) {
				String result = String.format("%s\n", record.toString());
				return result.getBytes();
			}
		});

		// execute program
		env.execute("Tumbling Window Edge");
	}

	private static class MetricAggergate
			implements AggregateFunction<Record, AggregateMetrics, AggregateMetrics> {
		@Override
		public AggregateMetrics createAccumulator() {
			return new AggregateMetrics( -1,0.0, 0, 0, 0.0, 0.0);
		}

		@Override
		public AggregateMetrics add(Record record, AggregateMetrics aggregateMetrics) {
			return new AggregateMetrics(
					record.getKey(),
					aggregateMetrics.getValue() + record.getValue(),
					aggregateMetrics.getSumTimestamps() + record.getArrivalTimestamp(),
					aggregateMetrics.getNumRecordsAggregated() + 1,
					record.getUnitDelayCost(),
					record.getUnitTrafficCost()
			);
		}

		@Override
		public AggregateMetrics getResult(AggregateMetrics aggregateMetrics) {
			return aggregateMetrics;
		}

		@Override
		public AggregateMetrics merge(AggregateMetrics aggregateMetrics1, AggregateMetrics aggregateMetrics2) {
			return new AggregateMetrics(
					aggregateMetrics1.getKey(),
					aggregateMetrics1.getValue() + aggregateMetrics2.getValue(),
					aggregateMetrics1.getSumTimestamps() + aggregateMetrics2.getSumTimestamps(),
					aggregateMetrics1.getNumRecordsAggregated() + aggregateMetrics2.getNumRecordsAggregated(),
					aggregateMetrics1.getDelayConstant(),
					aggregateMetrics1.getTrafficConstant()
			);
		}
	}

	private static class MyProcessWindowFunction extends ProcessWindowFunction<AggregateMetrics, EvictedRecord, Long, TimeWindow> {
		int edgeId;
		MyProcessWindowFunction(int edgeId) {
			this.edgeId = edgeId;
		}

		@Override
		public void process(Long key, Context context, Iterable<AggregateMetrics> iterable, Collector<EvictedRecord> out) throws Exception {
			AggregateMetrics aggregateMetrics = iterable.iterator().next();
			long currentProcessingTime = context.currentProcessingTime();
			long delay = (aggregateMetrics.getNumRecordsAggregated() * context.currentProcessingTime()) - aggregateMetrics.getSumTimestamps();
			double delayCost = delay * aggregateMetrics.getDelayConstant();
			long traffic = 1L;
			double trafficCost = traffic * aggregateMetrics.getTrafficConstant();

			out.collect(new EvictedRecord(
					key,
					aggregateMetrics.getValue(),
					delay,
					delayCost,
					traffic,
					trafficCost,
					-1,
					currentProcessingTime,
					aggregateMetrics.getNumRecordsAggregated(),
					this.edgeId,
					-1,
					-1,
					-1
			));
		}
	}
}
