package org.umn.dcs;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CentralAggregatorJob {

	public static void main(String[] args) throws Exception {

        System.out.println(String.format("arg0 = %s, arg1 = %s arg2 = %s arg3 = %s", args[0], args[1], args[2], args[3]));
	    String sourceHost = args[0];
        List<Integer> sourcePorts = Arrays
                .stream(args[1].split(","))
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        System.out.println(String.format("sourcePorts= %s", sourcePorts.toString()));

		String destinationHost = args[2];
		int destinationPort = Integer.parseInt(args[3]);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// add data sources
        		DataStream<String> dataSource1 = env.addSource(new CustomSocketTextStreamFunction(sourceHost, sourcePorts.get(0), "\n", 0));
        List<DataStream<String>> dataSourceList = new ArrayList<>();
        for (int index = 1; index < sourcePorts.size(); index++) {
            dataSourceList.add(env.addSource(new CustomSocketTextStreamFunction(sourceHost, sourcePorts.get(index), "\n", 0)));
        }

        DataStream<String> allSources = dataSource1.union(dataSourceList.toArray(new DataStream[dataSourceList.size()]));

		allSources
                .flatMap(new CenterRecordParserFlatMap())
				.writeToSocket(destinationHost, destinationPort, new SerializationSchema<String>() {
					@Override
					public byte[] serialize(String record) {
						return record.getBytes();
					}
				});

		// execute program
		env.execute("Center Aggregator");
	}
}
