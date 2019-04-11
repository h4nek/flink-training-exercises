package com.dataartisans.flinktraining.extras.applications;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.extras.datatypes.EnrichedRide;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

/**
 * This application builds upon {@link RideEnrichmentFlatMap} to produce the longest ride times for each starting cell.
 * <br><br>
 * It makes use of aggregation functions keyBy() and maxBy() as well as a new flatMap() that transforms the
 * stream elements.
 */
public class LongestRidesByCell {
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

        DataStream<EnrichedRide> enrichedNYCRides = rides
                .flatMap(new RideEnrichmentFlatMap.NYCEnrichment()); //

        DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides
                .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
                    @Override
                    public void flatMap(EnrichedRide ride,
                                        Collector<Tuple2<Integer, Minutes>> out) throws Exception {
                        if (!ride.isStart) {
                            Interval rideInterval = new Interval(ride.startTime, ride.endTime);
                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
                            out.collect(new Tuple2<>(ride.startCell, duration));
                        }
                    }
                });

        minutesByStartCell
                .keyBy(0) // startCell
                .maxBy(1) // duration
                .print();

        env.execute("Longest Taxi Rides By Cell");
    }
    
}
