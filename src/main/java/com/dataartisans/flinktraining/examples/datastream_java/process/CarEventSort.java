/*
 * Copyright 2017 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.examples.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.ConnectedCarEvent;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ConnectedCarAssigner;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.PriorityQueue;

/**
 * Sorts the ConnectedCarEvent events.
 * <br><br>
 * Link to get the data: https://training.ververica.com/exercises/connectedCar.html
 * */
public class CarEventSort {
    
    static final String carInOrderPath = "./carInOrder.csv";
    static final String carOutOfOrderPath = "./carOutOfOrder.csv";
    
    static final OutputTag<ConnectedCarEvent> lateCarEventsTag = new OutputTag<ConnectedCarEvent>("late") {};
    
	public static void main(String[] args) throws Exception {
	    
		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		String input = params.get("input", carInOrderPath);

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

		// connect to the data file
		DataStream<String> carData = env.readTextFile(input);

		// map to events
		DataStream<ConnectedCarEvent> events = carData
				.map((String line) -> ConnectedCarEvent.fromString(line))
				.assignTimestampsAndWatermarks(new ConnectedCarAssigner());

		// sort events
		SingleOutputStreamOperator<ConnectedCarEvent> sortedEvents = events
                .keyBy((ConnectedCarEvent event) -> event.carId)
				.process(new SortFunction());
//				.print();
		
        // get late events
        DataStream<ConnectedCarEvent> lateCarEvents = sortedEvents.getSideOutput(lateCarEventsTag);
        
        // print both outputs
        sortedEvents.print();
        lateCarEvents.printToErr();
        
		env.execute("Sort Connected Car Events");
	}

	public static class SortFunction extends KeyedProcessFunction<String, ConnectedCarEvent, ConnectedCarEvent> {
		private ValueState<PriorityQueue<ConnectedCarEvent>> queueState = null;

		@Override
		public void open(Configuration config) {
			ValueStateDescriptor<PriorityQueue<ConnectedCarEvent>> descriptor = new ValueStateDescriptor<>(
					// state name
					"sorted-events",
					// type information of state
					TypeInformation.of(new TypeHint<PriorityQueue<ConnectedCarEvent>>() {
					}));
			queueState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public void processElement(ConnectedCarEvent event, Context context, Collector<ConnectedCarEvent> out) throws Exception {
			TimerService timerService = context.timerService();

			if (context.timestamp() > timerService.currentWatermark()) {
				PriorityQueue<ConnectedCarEvent> queue = queueState.value();
				if (queue == null) {
					queue = new PriorityQueue<>(10);
				}
				queue.add(event);
				queueState.update(queue);
				timerService.registerEventTimeTimer(event.timestamp);
			}
			else {  // collect the late events into side output
			    context.output(lateCarEventsTag, event);
            }
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext context, Collector<ConnectedCarEvent> out) throws Exception {
			PriorityQueue<ConnectedCarEvent> queue = queueState.value();
			long watermark = context.timerService().currentWatermark();
			ConnectedCarEvent head = queue.peek();
			while (head != null && head.timestamp <= watermark) {
				out.collect(head);
				queue.remove(head);
				head = queue.peek();
			}
		}
	}
}