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

package com.dataartisans.flinktraining.exercises.datastream_java.state;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.CheckpointedTaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import scala.tools.nsc.doc.model.Val;

/**
 * The "Stateful Enrichment" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 * Parameters:
 * -rides path-to-input-file
 * -fares path-to-input-file
 *
 */
public class RidesAndFaresExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String ridesFile = params.get("rides", pathToRideData);
		final String faresFile = params.get("fares", pathToFareData);

		final int delay = 60;					// at most 60 seconds of delay
		final int servingSpeedFactor = 1800; 	// 30 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				//.addSource(rideSourceOrTest(new TaxiRideSource(ridesFile, delay, servingSpeedFactor)))
				.addSource(rideSourceOrTest(new CheckpointedTaxiRideSource(ridesFile, servingSpeedFactor)))
				.filter((TaxiRide ride) -> ride.isStart)
				.keyBy("rideId");

		DataStream<TaxiFare> fares = env
				//.addSource(fareSourceOrTest(new TaxiFareSource(faresFile, delay, servingSpeedFactor)))
				.addSource(fareSourceOrTest(new CheckpointedTaxiFareSource(faresFile, servingSpeedFactor)))
				.keyBy("rideId");

		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
				.connect(fares)
				.flatMap(new EnrichmentFunction());

		printOrTest(enrichedRides);

		env.execute("Join Rides with Fares (java RichCoFlatMap)");
	}

	public static class EnrichmentFunction extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {

	    private ValueState<TaxiRide> taxiRideValueState;
	    private ValueState<TaxiFare> taxiFareValueState;
	    
		@Override
		public void open(Configuration config) throws Exception {
            ValueStateDescriptor<TaxiRide> descriptor =
                    new ValueStateDescriptor<TaxiRide>("taxi ride", TaxiRide.class);
            ValueStateDescriptor<TaxiFare> descriptor2 =
                    new ValueStateDescriptor<TaxiFare>("taxi fare", TaxiFare.class);

            taxiRideValueState = getRuntimeContext().getState(descriptor);
            taxiFareValueState = getRuntimeContext().getState(descriptor2);
		}

		@Override
		public void flatMap1(TaxiRide ride, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
		    TaxiFare taxiFare = taxiFareValueState.value();
		    if (taxiFare != null) {
		        out.collect(new Tuple2<>(ride, taxiFare));
		        taxiFareValueState.clear();
            }
		    else {
		        taxiRideValueState.update(ride);
            }
		}

		@Override
		public void flatMap2(TaxiFare fare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
		    TaxiRide taxiRide = taxiRideValueState.value();
		    if (taxiRide != null) {
		        out.collect(new Tuple2<>(taxiRide, fare));
		        taxiRideValueState.clear();
            }
		    else {
		        taxiFareValueState.update(fare);
            }
		}
	}
}