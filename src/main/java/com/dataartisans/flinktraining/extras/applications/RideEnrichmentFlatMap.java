package com.dataartisans.flinktraining.extras.applications;

import com.dataartisans.flinktraining.exercises.datastream_java.basics.RideCleansingExercise;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.extras.datatypes.EnrichedRide;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * This application builds upon {@link RideCleansingExercise} and enriches all NY taxi rides with grid cell info
 * utilizing the flatMap() function.
 */
public class RideEnrichmentFlatMap {
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
                .flatMap(new NYCEnrichment());

        enrichedNYCRides.print();

        env.execute("Taxi Ride Enrichment (FlatMap)");
    }

    public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> valid = new RideCleansingExercise.NYCFilter();
            if (valid.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }
}
