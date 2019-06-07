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
import com.dataartisans.flinktraining.exercises.datastream_java.sources.FinSources;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is an event-time-based enrichment join implemented with a CoProcessFunction, used to enrich a
 * stream of financial Trades with Customer data.
 *
 * When we receive a Trade we want to join it with the newest Customer data that was knowable
 * at the time of the Trade. This means that we ignore Customer data with timestamps after the
 * the timestamp of the trade. And we wait until the current watermark reaches the timestamp
 * of the trade before doing the join, so that relevant Customer data will have arrived.
 *
 * Assuming the watermarking is correct, and there is no late data, this will behave deterministically.
 *
 * To keep this example simpler, the implementation assumes only one Trade or Customer update
 * per customerId, per timestamp (typically, per millisecond).
 *
 * There are tests in com.dataartisans.flinktraining.exercises.datastream_java.process.EventTimeJoinTest
 *
 * The goals of this exercise:
 *
 * (1) Extend this implementation to handle late Trades.
 *
 * (2) Implement a low-latency variant that immediately joins with available Customer data, if any, and
 * then emits an updated result if a Customer update arrives.
 *
 * (3) Extra credit: Extend the tests to cover this new functionality.
 */

public class EventTimeJoinExercise {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Simulated trade stream
		DataStream<Trade> tradeStream = FinSources.tradeSource(env);

		// Simulated customer stream
		DataStream<Customer> customerStream = FinSources.customerSource(env);

        final OutputTag<EnrichedTrade> outputTag = new OutputTag<EnrichedTrade>("side-output") {}; // for late trades

		// Stream of enriched trades
		SingleOutputStreamOperator<EnrichedTrade> joinedStream = tradeStream    // an extension of DataStream class
				.keyBy("customerId")
				.connect(customerStream.keyBy("customerId"))
//		        .process(new EventTimeJoinFunction());
				.process(new EventTimeLowLatencyJoinFunction());
		
		joinedStream.print();

        joinedStream.getSideOutput(outputTag).printToErr(); // print the late trades to stdErr

		env.execute("event-time join");
	}

	public static class EventTimeJoinFunction extends CoProcessFunction<Trade, Customer, EnrichedTrade> {
		// Store pending Trades for a customerId, keyed by timestamp
		private MapState<Long, Trade> tradeMap = null;

		// Store Customer updates for a customerId, keyed by timestamp
		private MapState<Long, Customer> customerMap = null;

        final OutputTag<EnrichedTrade> outputTag = new OutputTag<EnrichedTrade>("side-output") {}; // for late trades
		private int LATE_TRADES_THRESHOLD = 10000; // maximum time (in ms) to wait for potential late trades

		@Override
		public void open(Configuration config) {
			MapStateDescriptor<Long, Trade> tDescriptor = new MapStateDescriptor<>(
					"tradeBuffer",
					TypeInformation.of(Long.class),
					TypeInformation.of(Trade.class)
			);
			tradeMap = getRuntimeContext().getMapState(tDescriptor);

			MapStateDescriptor<Long, Customer> cDescriptor = new MapStateDescriptor<>(
					"customerBuffer",
					TypeInformation.of(Long.class),
					TypeInformation.of(Customer.class)
			);
			customerMap = getRuntimeContext().getMapState(cDescriptor);
		}

		@Override
		public void processElement1(Trade trade,
									Context context,
									Collector<EnrichedTrade> out)
				throws Exception {

			System.out.println("Received " + trade.toString());
			TimerService timerService = context.timerService();

			if (context.timestamp() > timerService.currentWatermark()) {
				// Do the join later, by which time any relevant Customer records should have arrived.
				tradeMap.put(trade.timestamp, trade);
				timerService.registerEventTimeTimer(trade.timestamp);
			} else { // Late Trades land here.
			    // we can join them right away
                context.output(outputTag, new EnrichedTrade(trade, getCustomerRecordToJoin(trade.timestamp)));
			}
		}

		@Override
		public void processElement2(Customer customer,
									Context context,
									Collector<EnrichedTrade> collector)
				throws Exception {

			System.out.println("Received " + customer.toString());
			customerMap.put(customer.timestamp, customer);

			/* Calling this solely for its side effect of freeing older Customer records.
			 * Otherwise Customers with frequent updates and no Trades would leak state.
			 */
			getCustomerRecordToJoin(context.timerService().currentWatermark());
		}

		@Override
		public void onTimer(long t,
							OnTimerContext context,
							Collector<EnrichedTrade> out)
				throws Exception {

			Trade trade = tradeMap.get(t);
			if (trade != null) {
				tradeMap.remove(t);
				EnrichedTrade joined = new EnrichedTrade(trade, getCustomerRecordToJoin(trade.timestamp));
				out.collect(joined);
			}
		}

		/**
		 * Returns the newest Customer that isn't newer than the Trade we are enriching.
		 * As a side effect, removes earlier Customer records that are no longer needed.
		 */
		private Customer getCustomerRecordToJoin(Long timestamp) throws Exception {
			Iterator<Map.Entry<Long, Customer>> customerEntries = customerMap.iterator();
			Customer theOneWeAreLookingFor = null;
			List<Long> toRemove = new ArrayList<>();

			while(customerEntries.hasNext()) {
				Customer c = customerEntries.next().getValue();
				// c should not be newer than the Trade being enriched
				if (c.timestamp <= timestamp) {
					/*
					* By the time Trades are being joined, they are being processed in order (by Timestamp).
					* This means that any Customer record too old to join with this Trade is too old
					* to be worth keeping any longer.
					*/
					if (theOneWeAreLookingFor != null) {
						if (c.timestamp > theOneWeAreLookingFor.timestamp) {
                            // added late trades support
                            if (c.timestamp > theOneWeAreLookingFor.timestamp + LATE_TRADES_THRESHOLD) {
                                toRemove.add(theOneWeAreLookingFor.timestamp);
                            }
							theOneWeAreLookingFor = c;
						}
					} else {
						theOneWeAreLookingFor = c;
					}
				}
			}

			for (Long t : toRemove) {
				System.out.println("Removing customer @ " + t);
				customerMap.remove(t);
			}

			return theOneWeAreLookingFor;
		}
	}

    public static class EventTimeLowLatencyJoinFunction extends CoProcessFunction<Trade, Customer, EnrichedTrade> {
        // Store pending Trades for a customerId, keyed by timestamp
        private MapState<Long, Trade> tradeMap = null;

        // Store Customer updates for a customerId, keyed by timestamp
        private MapState<Long, Customer> customerMap = null;
        
        // Store the realized immediate joins, keyed by the Trade being enriched (value is the given Customer update)
        private MapState<Trade, Customer> joinsMap = null;

        final OutputTag<EnrichedTrade> outputTag = new OutputTag<EnrichedTrade>("side-output") {}; // for late trades
        private int LATE_TRADES_THRESHOLD = 10000; // maximum time (in ms) to wait for potential late trades

        @Override
        public void open(Configuration config) {
            MapStateDescriptor<Long, Trade> tDescriptor = new MapStateDescriptor<>(
                    "tradeBuffer",
                    TypeInformation.of(Long.class),
                    TypeInformation.of(Trade.class)
            );
            tradeMap = getRuntimeContext().getMapState(tDescriptor);

            MapStateDescriptor<Long, Customer> cDescriptor = new MapStateDescriptor<>(
                    "customerBuffer",
                    TypeInformation.of(Long.class),
                    TypeInformation.of(Customer.class)
            );
            customerMap = getRuntimeContext().getMapState(cDescriptor);
            
            MapStateDescriptor<Trade, Customer> jDescriptor = new MapStateDescriptor<Trade, Customer>(
                    "joinsBuffer",
                    TypeInformation.of(Trade.class),
                    TypeInformation.of(Customer.class)
            );
            joinsMap = getRuntimeContext().getMapState(jDescriptor);
        }

        @Override
        public void processElement1(Trade trade,
                                    Context context,
                                    Collector<EnrichedTrade> out)
                throws Exception {

            System.out.println("Received " + trade.toString());
            TimerService timerService = context.timerService();

            if (context.timestamp() > timerService.currentWatermark()) {
                // Do the join now, and if any newer Customer updates arrive, also join later.
                // We can use the current trade's watermark as no Customer updates older than the newest one are 
                // expected to be useful (the onTimer method for any trades requiring those updates has already fired 
                // on the arrival of a newer Customer update)
                Customer newestCustomer = getCustomerRecordToJoin(trade.timestamp);
                if (newestCustomer != null) {
                    out.collect(new EnrichedTrade(trade, newestCustomer));
                    joinsMap.put(trade, newestCustomer);
                }
                tradeMap.put(trade.timestamp, trade);
                timerService.registerEventTimeTimer(trade.timestamp);
            } else { // Late Trades land here.
                // we can join them right away
                context.output(outputTag, new EnrichedTrade(trade, getCustomerRecordToJoin(trade.timestamp)));
            }
        }

        @Override
        public void processElement2(Customer customer,
                                    Context context,
                                    Collector<EnrichedTrade> collector)
                throws Exception {

            System.out.println("Received " + customer.toString());
            customerMap.put(customer.timestamp, customer);

            /* Calling this solely for its side effect of freeing older Customer records.
             * Otherwise Customers with frequent updates and no Trades would leak state.
             */
            getCustomerRecordToJoin(context.timerService().currentWatermark());
        }

        @Override
        public void onTimer(long t,
                            OnTimerContext context,
                            Collector<EnrichedTrade> out)
                throws Exception {

            Trade trade = tradeMap.get(t);
            if (trade != null) {
                tradeMap.remove(t);
                Customer newestCustomerUpdate = getCustomerRecordToJoin(trade.timestamp);
                if (newestCustomerUpdate != joinsMap.get(trade)) {  // check if we haven't made the same join at arrival
                    EnrichedTrade joined = new EnrichedTrade(trade, newestCustomerUpdate);
                    out.collect(joined);
                }
                joinsMap.remove(trade);
            }
        }

        /**
         * Returns the newest Customer that isn't newer than the Trade we are enriching.
         * As a side effect, removes earlier Customer records that are no longer needed.
         */
        private Customer getCustomerRecordToJoin(Long timestamp) throws Exception {
            Iterator<Map.Entry<Long, Customer>> customerEntries = customerMap.iterator();
            Customer theOneWeAreLookingFor = null;
            List<Long> toRemove = new ArrayList<>();

            while(customerEntries.hasNext()) {
                Customer c = customerEntries.next().getValue();
                // c should not be newer than the Trade being enriched
                if (c.timestamp <= timestamp) {
                    /*
                     * By the time Trades are being joined, they are being processed in order (by Timestamp).
                     * This means that any Customer record too old to join with this Trade is too old
                     * to be worth keeping any longer.
                     */
                    if (theOneWeAreLookingFor != null) {
                        if (c.timestamp > theOneWeAreLookingFor.timestamp) {
                            // added late trades support
                            if (c.timestamp > theOneWeAreLookingFor.timestamp + LATE_TRADES_THRESHOLD) {
                                toRemove.add(theOneWeAreLookingFor.timestamp);
                            }
                            theOneWeAreLookingFor = c;
                        }
                    } else {
                        theOneWeAreLookingFor = c;
                    }
                }
            }

            for (Long t : toRemove) {
                System.out.println("Removing customer @ " + t);
                customerMap.remove(t);
            }

            return theOneWeAreLookingFor;
        }
    }
}
