package com.dataartisans.flinktraining.exercises.datastream_java.broadcast;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.testing.TaxiRideTestBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.solutions.datastream_java.broadcast.OngoingRidesSolution;
import com.dataartisans.flinktraining.solutions.datastream_java.broadcast.TaxiQuerySolution;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Custom class testing the OngoingRidesExercise.
 * Currently the methods are written based on {@link TaxiQueryTest} without having further insight to their functionality.
 * 
 * Port 9999 needs to be manually open (ncat -lk 9999) for now.
 */
class OngoingRidesExerciseTest extends TaxiRideTestBase<TaxiRide> {
    
    /* TaxiQueryTest Part */
    
    static Testable javaExercise = () -> OngoingRidesExercise.main(new String[]{});

    @Test
    public void allOngoingRides() throws Exception {
        TaxiRide rideStarted = startRide(1, minutes(0), 0, 0, 1);

        TestRideSource rides = new TestRideSource(rideStarted);
        TestStringSource queries = new TestStringSource("0");

        List<String> expected = Lists.newArrayList(rideStarted.toString());
        assertEquals(expected, results(rides, queries));
    }

    private DateTime minutes(int n) {
        return new DateTime(2000, 1, 1, 0, 0).plusMinutes(n);
    }

    private TaxiRide testRide(long rideId, Boolean isStart, DateTime startTime, DateTime endTime, float startLon, float startLat, float endLon, float endLat, long taxiId) {
        return new TaxiRide(rideId, isStart, startTime, endTime, startLon, startLat, endLon, endLat, (short) 1, taxiId, 0);
    }
    
    private TaxiRide startRide(long rideId, DateTime startTime, float startLon, float startLat, long taxiId) {
        return testRide(rideId, true, startTime, new DateTime(0), startLon, startLat, 0, 0, taxiId);
    }

    private TaxiRide endRide(TaxiRide started, DateTime endTime, float endLon, float endLat) {
        return testRide(started.rideId, false, started.startTime, endTime, started.startLon, started.startLat, endLon, endLat, started.taxiId);
    }

    private List<String> results(TestRideSource rides, TestStringSource queries) throws Exception {
        Testable javaSolution = () -> OngoingRidesSolution.main(new String[]{});
        List<TaxiRide> results = runApp(rides, queries, new TestSink<>(), javaExercise, javaSolution);

        ArrayList<String> rideStrings = new ArrayList<>(results.size());
        results.iterator().forEachRemaining((TaxiRide t) ->
            rideStrings.add(t.toString()));
        return rideStrings;
    }
    
    /* Test Harness Part */

    @Test
    public void testAllOngoingRides() throws Exception {
        TwoInputStreamOperatorTestHarness<TaxiRide, String, TaxiRide> testHarness = setupHarness();
        
        // create data
        TaxiRide ride1 = startRide(1, minutes(1), 50, 14, 123);
        TaxiRide ride2 = startRide(2, minutes(0), 50.45689F, 52.6548F, 111);
        TaxiRide ride3 = startRide(3, minutes(150), -73.9947F, 40.750626F, 80085);
        String query1 = "0";
        
        // push the data in
        testHarness.processElement1(new StreamRecord<>(ride1, 100));
//        testHarness.processWatermark1(new Watermark(100));
        testHarness.processElement1(new StreamRecord<>(ride2, 200));
//        testHarness.processWatermark1(new Watermark(200));
        testHarness.processElement1(new StreamRecord<>(ride3, 50));
        testHarness.processWatermark1(new Watermark(500));
        testHarness.processElement2(new StreamRecord<>(query1, 500));
        testHarness.processWatermark2(new Watermark(500));
        
        // verify the state
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        
        expectedOutput.add(new StreamRecord<>(ride1, 100));
        expectedOutput.add(new StreamRecord<>(ride2, 200));
        expectedOutput.add(new StreamRecord<>(ride3, 50));
        expectedOutput.add(new Watermark(500));

        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();
        
        TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);
        
//        TaxiRide ride3 = endRide()
        testHarness.close();
    }
    
    private TwoInputStreamOperatorTestHarness<TaxiRide, String, TaxiRide> setupHarness() throws Exception {
        List<MapStateDescriptor<?, ?>> broadcastStateDescriptors = new ArrayList<>();
        broadcastStateDescriptors.add(new MapStateDescriptor<>(
                "dummy",
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        ));
        // instantiate operator
        CoBroadcastWithKeyedOperator<Long, TaxiRide, String, TaxiRide> operator =
                new CoBroadcastWithKeyedOperator<Long, TaxiRide, String, TaxiRide>(new OngoingRidesExercise.QueryFunction(), broadcastStateDescriptors);

        // setup test harness
        TwoInputStreamOperatorTestHarness<TaxiRide, String, TaxiRide> testHarness =
//                new KeyedTwoInputStreamOperatorTestHarness<>(operator,
//                        (TaxiRide t) -> t.taxiId,
//                        (String s) -> s,
//                        BasicTypeInfo.LONG_TYPE_INFO);
                new TwoInputStreamOperatorTestHarness<>(operator);

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }
    
    /* Own Custom Part */
    
    @Test
    void main() {
    }
    
    @Test
    void simpleTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);
        
        //DataStream<>
    }
}