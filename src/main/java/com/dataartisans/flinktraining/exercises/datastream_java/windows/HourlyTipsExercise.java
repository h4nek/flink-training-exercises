/*
 * Copyright 2018 data Artisans GmbH
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

package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
                .keyBy(x -> x.driverId)
                //.window(TumblingEventTimeWindows.of(Time.hours(1))); // a more expressive alternative
                .timeWindow(Time.hours(1)) // event time is used thanks to the environment characteristic
                .reduce(new HourlyTips())
                .timeWindowAll(Time.hours(1))
                .process(new HourlyMax());
        

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}
	
    public static class HourlyTips implements ReduceFunction<TaxiFare> {

        @Override
        public TaxiFare reduce(TaxiFare fare1, TaxiFare fare2) throws Exception {
            fare1.tip += fare2.tip; // we could create a new TaxiFare here and for example copy the contents of fare1, only changing the tip
            return fare1;
        }
    }
    
    public static class HourlyMax extends ProcessAllWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<TaxiFare> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            /* keeping the associated TaxiFare */
            TaxiFare maxFare = elements.iterator().next();
            
            for (TaxiFare element : elements) {
                if (element.tip > maxFare.tip) {
                    maxFare = element;
                }
            }
            
            out.collect(new Tuple3<>(context.window().getEnd(), maxFare.driverId, maxFare.tip));

            /* (alternative) copying the needed info */
//            float maxTip = Float.MIN_VALUE;
//            long driverID = 0;
//
//            for (TaxiFare element : elements) {
//                if (element.tip > maxTip) {
//                    maxTip = element.tip;
//                    driverID = element.driverId;
//                }
//            }
//
//            out.collect(new Tuple3<>(context.window().getEnd(), driverID, maxTip));
        }
    }
}