package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Trade;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

class ProcessingTimeJoinExerciseTest {

    @Test
    public void testOneCustomerInOrder() throws Exception {
        TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness();

        // create data
        Customer c0 = new Customer(0L, 0L, "customer0 - 0");
        Customer c1 = new Customer(2L, 0L, "customer0 - 2");
        Customer c2 = new Customer(5L, 0L, "customer0 - 5");

        Trade t0 = new Trade(1L, 0L, "trade0 - 1");
        Trade t1 = new Trade(4L, 0L, "trade0 - 4");
        Trade t2 = new Trade(5L, 0L, "trade0 - 5");

        EnrichedTrade et0 = new EnrichedTrade(t0, c0);
        EnrichedTrade et1 = new EnrichedTrade(t1, c1);
        EnrichedTrade et2 = new EnrichedTrade(t2, c2);

        // process the data
        testHarness.processElement2(new StreamRecord<>(c0, c0.timestamp));
        testHarness.processElement1(new StreamRecord<>(t0, t0.timestamp));
        testHarness.processElement2(new StreamRecord<>(c1, c1.timestamp));
        testHarness.processElement1(new StreamRecord<>(t1, t1.timestamp));
        testHarness.processElement2(new StreamRecord<>(c2, c2.timestamp));
        testHarness.processElement1(new StreamRecord<>(t2, t2.timestamp));

        // check the output
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>(et0, t0.timestamp));
        expectedOutput.add(new StreamRecord<>(et1, t1.timestamp));
        expectedOutput.add(new StreamRecord<>(et2, t2.timestamp));

        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

        TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

        testHarness.close();
    }

    @Test
    public void testOneCustomerOutOfOrder() throws Exception {
        TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness();

        // create data
        Customer c0 = new Customer(0L, 0L, "customer0 - 0");
        Customer c1 = new Customer(2L, 0L, "customer0 - 2");
        Customer c2 = new Customer(6L, 0L, "customer0 - 5");

        Trade t0 = new Trade(1L, 0L, "trade0 - 1");
        Trade t1 = new Trade(4L, 0L, "trade0 - 4");
        Trade t2 = new Trade(5L, 0L, "trade0 - 5");

        EnrichedTrade et0 = new EnrichedTrade(t0, c0);
        EnrichedTrade et1 = new EnrichedTrade(t1, c0);
        EnrichedTrade et2 = new EnrichedTrade(t2, c2);

        // process the data
        testHarness.processElement2(new StreamRecord<>(c0, c0.timestamp));
        testHarness.processElement1(new StreamRecord<>(t0, t0.timestamp));
        testHarness.processElement1(new StreamRecord<>(t1, t1.timestamp));  // arrives before earlier customer update
        testHarness.processElement2(new StreamRecord<>(c1, c1.timestamp));
        testHarness.processElement2(new StreamRecord<>(c2, c2.timestamp));
        testHarness.processElement1(new StreamRecord<>(t2, t2.timestamp));  // arrives after later customer update

        // check the output
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>(et0, t0.timestamp));
        expectedOutput.add(new StreamRecord<>(et1, t1.timestamp));
        expectedOutput.add(new StreamRecord<>(et2, t2.timestamp));

        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

        TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

        testHarness.close();
    }

    @Test
    public void testMultipleCustomersOutOfOrder() throws Exception {
        TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness();

        // create data
        Customer c0 = new Customer(0L, 0L, "customer0 - 0");
        Customer c1 = new Customer(1L, 1L, "customer1 - 1");
        Customer c2 = new Customer(5L, 0L, "customer0 - 5");
        Customer c3 = new Customer(6L, 1L, "customer1 - 6");
        Customer c4 = new Customer(7L, 2L, "customer2 - 7");
        Customer c5 = new Customer(8L, 1L, "customer1 - 8");

        Trade t0 = new Trade(1L, 0L, "trade0 - 1");
        Trade t1 = new Trade(4L, 0L, "trade0 - 4");
        Trade t2 = new Trade(5L, 1L, "trade1 - 5");
        Trade t3 = new Trade(5L, 0L, "trade0 - 5");
        Trade t4 = new Trade(10L, 1L, "trade1 - 10");
        Trade t5 = new Trade(12L, 2L, "trade 2 - 12");

        EnrichedTrade et0 = new EnrichedTrade(t0, null);
        EnrichedTrade et1 = new EnrichedTrade(t1, c0);
        EnrichedTrade et2 = new EnrichedTrade(t2, c3);
        EnrichedTrade et3 = new EnrichedTrade(t3, c2);
        EnrichedTrade et4 = new EnrichedTrade(t4, c3);
        EnrichedTrade et5 = new EnrichedTrade(t5, c4);

        // process the data
        testHarness.processElement1(new StreamRecord<>(t0, t0.timestamp));  // arrives before any customer info
        testHarness.processElement2(new StreamRecord<>(c0, c0.timestamp));
        testHarness.processElement1(new StreamRecord<>(t1, t1.timestamp));
        testHarness.processElement2(new StreamRecord<>(c1, c1.timestamp));
        testHarness.processElement2(new StreamRecord<>(c2, c2.timestamp));
        testHarness.processElement2(new StreamRecord<>(c3, c3.timestamp));
        testHarness.processElement1(new StreamRecord<>(t2, t2.timestamp));  // arrives after later customer update
        testHarness.processElement1(new StreamRecord<>(t3, t3.timestamp));
        testHarness.processElement2(new StreamRecord<>(c4, c4.timestamp));
        testHarness.processElement1(new StreamRecord<>(t4, t4.timestamp));  // arrives before earlier customer update
        testHarness.processElement1(new StreamRecord<>(t5, t5.timestamp));
        testHarness.processElement2(new StreamRecord<>(c5, c5.timestamp));

        // check the output
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>(et0, t0.timestamp));
        expectedOutput.add(new StreamRecord<>(et1, t1.timestamp));
        expectedOutput.add(new StreamRecord<>(et2, t2.timestamp));
        expectedOutput.add(new StreamRecord<>(et3, t3.timestamp));
        expectedOutput.add(new StreamRecord<>(et4, t4.timestamp));
        expectedOutput.add(new StreamRecord<>(et5, t5.timestamp));

        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

        TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

        testHarness.close();
    }

    private TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> setupHarness() throws Exception {
        KeyedCoProcessOperator<Long, Trade, Customer, EnrichedTrade> operator = new KeyedCoProcessOperator<>(
                new ProcessingTimeJoinExercise.ProcessingTimeJoinFunction());

        TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness
                = new KeyedTwoInputStreamOperatorTestHarness<>(
                operator,
                (Trade t) -> t.customerId,
                (Customer c) -> c.customerId,
                BasicTypeInfo.LONG_TYPE_INFO
        );

        testHarness.setup();
        testHarness.open();

        return testHarness;
    }

    /* Custom Tests */
    private StreamExecutionEnvironment setUp() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }

    private DataStream<EnrichedTrade> joinStreams(DataStream<Trade> tradeStream, DataStream<Customer> customerStream) {
        return tradeStream
                .keyBy("customerId")
                .connect(customerStream.keyBy("customerId"))
                .process(new ProcessingTimeJoinExercise.ProcessingTimeJoinFunction());
    }

//    /**
//     * java.io.NotSerializableException
//     * 
//     * @throws Exception
//     */
//    @Test
//    public void simpleTest() throws Exception {
//        StreamExecutionEnvironment env = setUp();
//        
//        //create data
//        Customer c0 = new Customer(0L, 0L, "customer0 - 0");
//        Customer c1 = new Customer(1L, 1L, "customer1 - 1");
//        Customer c2 = new Customer(5L, 0L, "customer0 - 5");
//        
//        Trade t0 = new Trade(1L, 0L, "trade0 - 1");
//        Trade t1 = new Trade(4L, 0L, "trade0 - 4");
//        Trade t2 = new Trade(5L, 1L, "trade1 - 5");
//        Trade t3 = new Trade(5L, 0L, "trade0 - 5");
//        
//        // set up streams
//
//        DataStream<Customer> customerStream = env.addSource(new SourceFunction<Customer>() {
//            private volatile boolean running = true;
//            
//            @Override
//            public void run(SourceContext<Customer> ctx) throws Exception {
//                ctx.collectWithTimestamp(c0, c0.timestamp);
//                ctx.emitWatermark(new Watermark(c0.timestamp));
//                Thread.sleep(1000);
//                
//                ctx.collectWithTimestamp(c1, c1.timestamp);
//                ctx.emitWatermark(new Watermark(c1.timestamp));
//                Thread.sleep(2000);
//
//                ctx.collectWithTimestamp(c2, c2.timestamp);
//                ctx.emitWatermark(new Watermark(c2.timestamp));
//                Thread.sleep(2500);
//                
//                while (running) {
//                    Thread.sleep(1000);
//                }
//            }
//
//            @Override
//            public void cancel() {
//                running = false;
//            }
//        });
//
//        DataStream<Trade> tradeStream = env.addSource(new SourceFunction<Trade>() {
//            private volatile boolean running = true;
//
//            @Override
//            public void run(SourceContext<Trade> ctx) throws Exception {
//                ctx.collectWithTimestamp(t0, t0.timestamp);
//                ctx.emitWatermark(new Watermark(t0.timestamp));
//                Thread.sleep(1000);
//
//                ctx.collectWithTimestamp(t1, t1.timestamp);
//                ctx.emitWatermark(new Watermark(t1.timestamp));
//                Thread.sleep(2000);
//
//                ctx.collectWithTimestamp(t2, t2.timestamp);
//                ctx.emitWatermark(new Watermark(t2.timestamp));
//                Thread.sleep(2500);
//
//                ctx.collectWithTimestamp(t3, t3.timestamp);
//                ctx.emitWatermark(new Watermark(t3.timestamp));
//                Thread.sleep(2500);
//
//                while (running) {
//                    Thread.sleep(1000);
//                }
//            }
//
//            @Override
//            public void cancel() {
//                running = false;
//            }
//        });
//        
//        DataStream<EnrichedTrade> joinedStream = joinStreams(tradeStream, customerStream);
//        joinedStream.printToErr();
//        
//        env.execute("processing-time join test");
//       // assertTrue(true);
//    }
}