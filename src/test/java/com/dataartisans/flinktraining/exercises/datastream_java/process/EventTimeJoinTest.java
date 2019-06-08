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

package com.dataartisans.flinktraining.exercises.datastream_java.process;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Customer;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedTrade;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.Trade;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

public class EventTimeJoinTest {

	@Test
	public void testTradeBeforeCustomer() throws Exception {
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness(
		        new EventTimeJoinExercise.EventTimeJoinFunction());
		int timerCountBefore;

		// push in data
		Customer c0 = new Customer(0L, 0L, "customer-0");

		Trade t1000 = new Trade(1000L, 0L, "trade-1000");

		Customer c0500 = new Customer(500L, 0L, "customer-500");
		Customer c1500 = new Customer(1500L, 0L, "customer-1500");

		testHarness.processElement2(new StreamRecord<>(c0, 0));
		testHarness.processWatermark2(new Watermark(0));

		// processing a Trade should create an event-time timer
		timerCountBefore = testHarness.numEventTimeTimers();
		testHarness.processElement1(new StreamRecord<>(t1000, 1000));
		assert(testHarness.numEventTimeTimers() == timerCountBefore + 1);

		testHarness.processWatermark1(new Watermark(1000));

		testHarness.processElement2(new StreamRecord<>(c0500, 500));
		testHarness.processWatermark2(new Watermark(500));

		testHarness.processElement2(new StreamRecord<>(c1500, 1500));

		// now the timer should fire
		timerCountBefore = testHarness.numEventTimeTimers();
		testHarness.processWatermark2(new Watermark(1500));
		assert(testHarness.numEventTimeTimers() == timerCountBefore - 1);

		// verify operator state
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		EnrichedTrade et1000 = new EnrichedTrade(t1000, c0500);

		expectedOutput.add(new Watermark(0L));
		expectedOutput.add(new Watermark(500L));
		expectedOutput.add(new StreamRecord<>(et1000, 1000L));
		expectedOutput.add(new Watermark(1000L));

		ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

		testHarness.close();
	}

	@Test
	public void testTradeAfterCustomer() throws Exception {
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness(
                new EventTimeJoinExercise.EventTimeJoinFunction());

		// push in data
		Customer c0500 = new Customer(500L, 0L, "customer-500");
		Customer c1500 = new Customer(1500L, 0L, "customer-1500");

		Trade t1200 = new Trade(1200L, 0L, "trade-1200");
		Trade t1500 = new Trade(1500L, 0L, "trade-1500");

		testHarness.processElement2(new StreamRecord<>(c0500, 500));
		testHarness.processWatermark2(new Watermark(500));

		testHarness.processElement2(new StreamRecord<>(c1500, 1500));
		testHarness.processWatermark2(new Watermark(1500));

		testHarness.processElement1(new StreamRecord<>(t1200, 1200));
		testHarness.processWatermark1(new Watermark(1200));

		testHarness.processElement1(new StreamRecord<>(t1500, 1500));
		testHarness.processWatermark1(new Watermark(1500));

		// verify operator state
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		EnrichedTrade et1200 = new EnrichedTrade(t1200, c0500);
		EnrichedTrade et1500 = new EnrichedTrade(t1500, c1500);

		expectedOutput.add(new StreamRecord<>(et1200, 1200L));
		expectedOutput.add(new Watermark(1200L));
		expectedOutput.add(new StreamRecord<>(et1500, 1500L));
		expectedOutput.add(new Watermark(1500L));

		ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

		testHarness.close();
	}

	@Test
	public void testManyPendingTrades() throws Exception {
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness(
		        new EventTimeJoinExercise.EventTimeJoinFunction());

		// push in data
		Trade t1700 = new Trade(1700L, 0L, "trade-1700");
		Trade t1800 = new Trade(1800L, 0L, "trade-1800");
		Trade t2000 = new Trade(2000L, 0L, "trade-2000");

		Customer c1600 = new Customer(1600L, 0L, "customer-1600");
		Customer c2100 = new Customer(2100L, 0L, "customer-2100");

		testHarness.processElement1(new StreamRecord<>(t1700, 1700));
		testHarness.processWatermark1(new Watermark(1700));

		testHarness.processElement1(new StreamRecord<>(t1800, 1800));
		testHarness.processWatermark1(new Watermark(1800));

		testHarness.processElement1(new StreamRecord<>(t2000, 2000));
		testHarness.processWatermark1(new Watermark(2000));

		testHarness.processElement2(new StreamRecord<>(c1600, 1600));
		testHarness.processWatermark2(new Watermark(1600));

		testHarness.processElement2(new StreamRecord<>(c2100, 2100));
		testHarness.processWatermark2(new Watermark(2100));

		// verify operator state
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		EnrichedTrade et1700 = new EnrichedTrade(t1700, c1600);
		EnrichedTrade et1800 = new EnrichedTrade(t1800, c1600);
		EnrichedTrade et2000 = new EnrichedTrade(t2000, c1600);

		expectedOutput.add(new Watermark(1600L));
		expectedOutput.add(new StreamRecord<>(et1700, 1700L));
		expectedOutput.add(new StreamRecord<>(et1800, 1800L));
		expectedOutput.add(new StreamRecord<>(et2000, 2000L));
		expectedOutput.add(new Watermark(2000L));

		ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);

		testHarness.close();
	}
	
	@Test
    public void testLateTrades() throws Exception {
        TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness(
                new EventTimeJoinExercise.EventTimeJoinFunction());

        final OutputTag<EnrichedTrade> outputTag = new OutputTag<EnrichedTrade>("side-output") {}; // for late trades

        // push in data
        Trade t1700 = new Trade(1700L, 0L, "trade-1700");
        Trade t1800 = new Trade(1800L, 0L, "trade-1800");
        Trade t1900 = new Trade(1900L, 0L, "trade-1900");
        Trade t2000 = new Trade(2000L, 0L, "trade-2000");
        Trade t2200 = new Trade(2200L, 0L, "trade-2200");
        Trade t2300 = new Trade(2300L, 0L, "trade-2300");

        Customer c1600 = new Customer(1600L, 0L, "customer-1600");
        Customer c1750 = new Customer(1750L, 0L, "customer-1750");
        Customer c1850 = new Customer(1850L, 0L, "customer-1850");
        Customer c2100 = new Customer(2100L, 0L, "customer-2100");
        Customer c2400 = new Customer(2400L, 0L, "customer-2400");

        testHarness.processElement1(new StreamRecord<>(t1700, 1700));
        testHarness.processWatermark1(new Watermark(1700));

        testHarness.processElement1(new StreamRecord<>(t2000, 2000));
        testHarness.processWatermark1(new Watermark(2000));

        testHarness.processElement2(new StreamRecord<>(c1600, 1600));
        testHarness.processWatermark2(new Watermark(1600));

        testHarness.processElement2(new StreamRecord<>(c1750, 1750));
        testHarness.processWatermark2(new Watermark(1750));

        testHarness.processElement2(new StreamRecord<>(c1850, 1850));
        testHarness.processWatermark2(new Watermark(1850));
        
        testHarness.processElement2(new StreamRecord<>(c2100, 2100));
        testHarness.processWatermark2(new Watermark(2100));

        testHarness.processElement1(new StreamRecord<>(t2200, 2200));
        testHarness.processWatermark1(new Watermark(2200));
        
        testHarness.processElement1(new StreamRecord<>(t1800, 1800));   // a late trade
//        testHarness.processWatermark1(new Watermark(1800));   // sending in old Watermarks would mess up the event time

        testHarness.processElement2(new StreamRecord<>(c2400, 2400));
        testHarness.processWatermark2(new Watermark(2400));
        
        testHarness.processElement1(new StreamRecord<>(t1900, 1900)); // another late trade
//        testHarness.processWatermark1(new Watermark(1900));

        testHarness.processElement1(new StreamRecord<>(t2300, 2300));
        testHarness.processWatermark1(new Watermark(2300));
        
        // verify operator state
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<StreamRecord<EnrichedTrade>> expectedSideOutput = new ConcurrentLinkedQueue<>();

        EnrichedTrade et1700 = new EnrichedTrade(t1700, c1600);
        EnrichedTrade et1800 = new EnrichedTrade(t1800, c1750);
        EnrichedTrade et1900 = new EnrichedTrade(t1900, c1850);
        EnrichedTrade et2000 = new EnrichedTrade(t2000, c1850);
        EnrichedTrade et2200 = new EnrichedTrade(t2200, c2100);
        EnrichedTrade et2300 = new EnrichedTrade(t2300, c2100);

        expectedOutput.add(new Watermark(1600L));
        expectedOutput.add(new StreamRecord<>(et1700, 1700L));
        expectedOutput.add(new Watermark(1750L));
        expectedOutput.add(new Watermark(1850L));
        expectedOutput.add(new StreamRecord<>(et2000, 2000L));
        expectedOutput.add(new Watermark(2000L));
        expectedOutput.add(new Watermark(2100L));
        expectedSideOutput.add(new StreamRecord<>(et1800, 1800));
        expectedOutput.add(new StreamRecord<>(et2200, 2200));
        expectedOutput.add(new Watermark(2200));
        expectedSideOutput.add(new StreamRecord<>(et1900, 1900));
        expectedOutput.add(new StreamRecord<>(et2300, 2300));
        expectedOutput.add(new Watermark(2300));
        
        

        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();
        ConcurrentLinkedQueue<StreamRecord<EnrichedTrade>> actualSideOutput = testHarness.getSideOutput(outputTag);

        System.out.println("\nEnriched Trades");
        System.out.println(expectedOutput);
        System.out.println(actualOutput);

        System.out.println("\nLate Trades");
        System.out.println(expectedSideOutput);
        System.out.println(actualSideOutput);
        
        TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);
        TestHarnessUtil.assertOutputEquals("Side output was not correct.", expectedSideOutput, actualSideOutput);
        
        testHarness.close();
    }
    
    @Test
    public void testLowLatencyJoinFunction() throws Exception {
	    TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness = setupHarness(
	            new EventTimeJoinExercise.EventTimeLowLatencyJoinFunction());

        // push in data
        Trade t2300 = new Trade(2300L, 0L, "trade-2300");
        Trade t2500 = new Trade(2500L, 0L, "trade-2500");
        Trade t2800 = new Trade(2800L, 0L, "trade-2800");
        Trade t2900 = new Trade(2900L, 0L, "trade-2900");
        Trade t3300 = new Trade(3300L, 0L, "trade-3300");
        Trade t3500 = new Trade(3500L, 0L, "trade-3500");

        
        Customer c2200 = new Customer(2200L, 0L, "customer-2200");
        Customer c2600 = new Customer(2600L, 0L, "customer-2600");
        Customer c3000 = new Customer(3000L, 0L, "customer-3000");
        Customer c3200 = new Customer(3200L, 0L, "customer-3200");
        Customer c3400 = new Customer(3400L, 0L, "customer-3400");
        
        testHarness.processElement1(new StreamRecord<>(t2300, 2300));
        testHarness.processWatermark1(new Watermark(2300));

        testHarness.processElement1(new StreamRecord<>(t2500, 2500));
        testHarness.processWatermark1(new Watermark(2500));

        testHarness.processElement2(new StreamRecord<>(c2200, 2200));
        testHarness.processWatermark2(new Watermark(2200));

        testHarness.processElement2(new StreamRecord<>(c2600, 2600));
        testHarness.processWatermark2(new Watermark(2600));

        testHarness.processElement1(new StreamRecord<>(t2800, 2800));
        testHarness.processWatermark1(new Watermark(2800));

        testHarness.processElement2(new StreamRecord<>(c3000, 3000));
        testHarness.processWatermark2(new Watermark(3000));

        testHarness.processElement1(new StreamRecord<>(t2900, 2900));
        testHarness.processWatermark1(new Watermark(2900));

        testHarness.processElement1(new StreamRecord<>(t3300, 3300));
        testHarness.processWatermark1(new Watermark(3300));

        testHarness.processElement2(new StreamRecord<>(c3200, 3200));
        testHarness.processWatermark2(new Watermark(3200));

        testHarness.processElement1(new StreamRecord<>(t3500, 3500));
        testHarness.processWatermark1(new Watermark(3500));

        testHarness.processElement2(new StreamRecord<>(c3400, 3400));
        testHarness.processWatermark2(new Watermark(3400));
        
        // verify output
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        
        EnrichedTrade et2300 = new EnrichedTrade(t2300, c2200); // we don't expect any immediate joins here because no customer updates have arrived yet
        EnrichedTrade et2500 = new EnrichedTrade(t2500, c2200);
        EnrichedTrade eti2800 = new EnrichedTrade(t2800, c2600);    // this immediate join will be the same as the regular join, so we don't expect duplicates
        EnrichedTrade eti2900 = new EnrichedTrade(t2900, c2600);    // at the time this immediate join happens, we could already do the regular join
        EnrichedTrade eti3300 = new EnrichedTrade(t3300, c3000);
        EnrichedTrade eti3500 = new EnrichedTrade(t3500, c3200);    // the regular join won't happen because no customer record will arrive that would carry a bigger watermark than this trade 
        EnrichedTrade et3300 = new EnrichedTrade(t3300, c3200);     // the regular join happens after receiving a more recent customer update (c3400)

        expectedOutput.add(new Watermark(2200));
        expectedOutput.add(new StreamRecord<>(et2300, 2300));
        expectedOutput.add(new StreamRecord<>(et2500, 2500));
        expectedOutput.add(new Watermark(2500));
        expectedOutput.add(new StreamRecord<>(eti2800, 2800));
        expectedOutput.add(new Watermark(2600));
        expectedOutput.add(new Watermark(2800));
        expectedOutput.add(new StreamRecord<>(eti2900, 2900));
        expectedOutput.add(new Watermark(2900));
        expectedOutput.add(new StreamRecord<>(eti3300, 3300));
        expectedOutput.add(new Watermark(3000));
        expectedOutput.add(new Watermark(3200));
        expectedOutput.add(new StreamRecord<>(eti3500, 3500));
        expectedOutput.add(new StreamRecord<>(et3300, 3300));
        expectedOutput.add(new Watermark(3400));
        
        ConcurrentLinkedQueue<Object> actualOutput = testHarness.getOutput();

        System.out.println("\nActual Output");
        System.out.println(actualOutput);
        
        TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, actualOutput);
        
        testHarness.close();
    }

	private TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> setupHarness(
	        CoProcessFunction<Trade, Customer, EnrichedTrade> eventTimeJoinFunction) throws Exception {
		// instantiate operator
		KeyedCoProcessOperator<Long, Trade, Customer, EnrichedTrade> operator =
				new KeyedCoProcessOperator<>(eventTimeJoinFunction);

		// setup test harness
		TwoInputStreamOperatorTestHarness<Trade, Customer, EnrichedTrade> testHarness =
				new KeyedTwoInputStreamOperatorTestHarness<>(operator,
						(Trade t) -> t.customerId,
						(Customer c) -> c.customerId,
						BasicTypeInfo.LONG_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		return testHarness;
	}
}
