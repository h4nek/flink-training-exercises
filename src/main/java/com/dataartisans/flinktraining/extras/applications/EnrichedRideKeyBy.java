package com.dataartisans.flinktraining.extras.applications;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.extras.datatypes.EnrichedRide;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * This demo shows various basic usages of keyBy() function, building upon {@link RideEnrichmentFlatMap}.
 */
public class EnrichedRideKeyBy {
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

        /* basic expensive method */
//        KeyedStream<EnrichedRide, Tuple> enrichedNYCRides = rides
//                .flatMap(new RideEnrichmentFlatMap.NYCEnrichment())
//                .keyBy("startCell");

        /* effective method */
//        KeyedStream<EnrichedRide, Integer> enrichedNYCRides = rides
//                .flatMap(new RideEnrichmentFlatMap.NYCEnrichment())
//                .keyBy(
//                        new KeySelector<EnrichedRide, Integer>() {
//                            @Override
//                            public Integer getKey(EnrichedRide ride) throws Exception {
//                                return ride.startCell;
//                            }
//                        });

        /* simplified effective method */
        KeyedStream<EnrichedRide, Integer> enrichedNYCRides = rides
                .flatMap(new RideEnrichmentFlatMap.NYCEnrichment())
                .keyBy(ride -> ride.startCell);

        enrichedNYCRides.print();

        env.execute("Enriched Taxi Ride KeyBy");
    }
}
