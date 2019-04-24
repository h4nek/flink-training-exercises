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

package com.dataartisans.flinktraining.exercises.datastream_java.broadcast;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Random;

/**
 * The "Nearest Future Taxi" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * Given a location that is broadcast, the goal of this exercise is to watch the stream of
 * taxi rides and report on taxis that complete rides closest to the requested location.
 * The application should be able to handle simultaneous queries.
 *
 * Parameters:
 * -input path-to-input-file
 *
 * Use nc -lk 9999 to establish a socket stream from stdin on port 9999
 * (ncat -lk 9999 on Windows. https://nmap.org/ncat/)
 *
 * Some good locations:
 *
 *   -74, 41 					(Near, but outside the city to the NNW)
 *   -73.7781, 40.6413 			(JFK Airport)
 *   -73.977664, 40.761484		(Museum of Modern Art)
 */
public class NearestTaxiExercise extends ExerciseBase {

	private static class Query {

		private final long queryId;
		private final float longitude;
		private final float latitude;

		Query(final float longitude, final float latitude) {
			this.queryId = new Random().nextLong();
			this.longitude = longitude;
			this.latitude = latitude;
		}

		Long getQueryId() {
			return queryId;
		}

		public float getLongitude() {
			return longitude;
		}

		public float getLatitude() {
			return latitude;
		}

		@Override
		public String toString() {
			return "Query{" +
					"id=" + queryId +
					", longitude=" + longitude +
					", latitude=" + latitude +
					'}';
		}
	}

	final static MapStateDescriptor queryDescriptor = new MapStateDescriptor<>(
			"queries",
			BasicTypeInfo.LONG_TYPE_INFO,
			TypeInformation.of(Query.class));

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       	// events are out of order by at most 60 seconds
		final int servingSpeedFactor = 600; 	// 10 minutes worth of events are served every second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// add a socket source
		BroadcastStream<Query> queryStream = env.socketTextStream("localhost", 9999)
				.map(new MapFunction<String, Query>() {
					@Override
					public Query map(String msg) throws Exception {
						String[] parts = msg.split(",\\s*");
						return new Query(
								Float.valueOf(parts[0]),	// longitude
								Float.valueOf(parts[1]));	// latitude
					}
				})
				.broadcast(queryDescriptor);

		DataStream<Tuple3<Long, Long, Float>> reports = rides
				.keyBy((TaxiRide ride) -> ride.taxiId)
				.connect(queryStream)
				.process(new QueryFunction());

		DataStream<Tuple3<Long, Long, Float>> nearest = reports
				// key by the queryId
				.keyBy(new KeySelector<Tuple3<Long, Long, Float>, Long>() {
					@Override
					public Long getKey(Tuple3<Long, Long, Float> value) throws Exception {
						return value.f0;
					}
				})
				.process(new ClosestTaxi());

		printOrTest(nearest);

		env.execute("Nearest Available Taxi");
	}

	// Only pass thru values that are new minima -- remove duplicates.
	public static class ClosestTaxi extends KeyedProcessFunction<Long, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>> {
		// store (taxiId, distance), keyed by queryId
		private transient ValueState<Tuple2<Long, Float>> closest;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<Tuple2<Long, Float>> descriptor =
					new ValueStateDescriptor<Tuple2<Long, Float>>(
							// state name
							"report",
							// type information of state
							TypeInformation.of(new TypeHint<Tuple2<Long, Float>>() {}));
			closest = getRuntimeContext().getState(descriptor);
		}

		@Override
		// in and out tuples: (queryId, taxiId, distance)
		public void processElement(Tuple3<Long, Long, Float> report, Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			if (closest.value() == null || report.f2 < closest.value().f1) {
				closest.update(new Tuple2<>(report.f1, report.f2));
				out.collect(report);
			}
		}
	}

	// Note that in order to have consistent results after a restore from a checkpoint, the
	// behavior of this method must be deterministic, and NOT depend on characteristics of an
	// individual sub-task.
	public static class QueryFunction extends KeyedBroadcastProcessFunction<Long, TaxiRide, Query, Tuple3<Long, Long, Float>> {

		@Override
		public void processBroadcastElement(Query query, Context ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			System.out.println("new query " + query);
			ctx.getBroadcastState(queryDescriptor).put(query.getQueryId(), query);
		}

		@Override
		// Output (queryId, taxiId, euclidean distance) for every query, if the taxi ride is now ending.
		public void processElement(TaxiRide ride, ReadOnlyContext ctx, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			if (!ride.isStart) {
                Iterable<Map.Entry<Long, Query>> broadcastState = ctx.getBroadcastState(queryDescriptor).immutableEntries();
                for (Map.Entry<Long, Query> mapEntry : broadcastState) {
//                    float dist = getEuclideanDistance(mapEntry.getValue(), ride);
                    Query query = mapEntry.getValue();
                    float dist = (float) ride.getEuclideanDistance(query.getLongitude(), query.getLatitude());
                    out.collect(new Tuple3<>(mapEntry.getKey(), ride.taxiId, dist));
                }
			}
		}
		
		// A basic implementation of computing the euclidean distance. Might be unsuitable for the task.
		private float getEuclideanDistance(Query query, TaxiRide ride) {
		    float latDist = Math.abs(query.latitude - ride.endLat);
		    float lonDist = Math.abs(query.longitude - ride.endLon);
		    return (float) Math.sqrt(Math.pow(latDist, 2) + Math.pow(lonDist, 2));
        }
	}
}