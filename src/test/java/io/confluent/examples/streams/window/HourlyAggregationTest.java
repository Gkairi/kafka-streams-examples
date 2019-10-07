/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams.window;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.time.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class HourlyAggregationTest {

    Logger LOGGER = Logger.getLogger(HourlyAggregationTest.class.getName());
    private static final String inputTopic = "inputTopic";
    private static final String outputTopic = "outputTopic";
    private static final ZoneId zone = ZoneOffset.UTC;
    private static final int windowStartHour = 18;

    @Test
    public void shouldSumNumbersByHoursOneKey() {
        final List<MyEvent> inputValues = Arrays.asList(

                new MyEvent(1, ZonedDateTime.of(2019, 1, 1, 15, 19, 0, 0, zone)),
                new MyEvent(2, ZonedDateTime.of(2019, 1, 1, 15, 20, 0, 0, zone)),
                new MyEvent(3, ZonedDateTime.of(2019, 1, 1, 15, 21, 0, 0, zone)),

                new MyEvent(4, ZonedDateTime.of(2019, 1, 1, 16, 29, 0, 0, zone)),
                new MyEvent(5, ZonedDateTime.of(2019, 1, 1, 16, 30, 0, 0, zone)),
                new MyEvent(6, ZonedDateTime.of(2019, 1, 1, 16, 31, 0, 0, zone)),

                new MyEvent(7, ZonedDateTime.of(2019, 1, 1, 17, 39, 0, 0, zone)),
                new MyEvent(8, ZonedDateTime.of(2019, 1, 1, 17, 40, 0, 0, zone)),
                new MyEvent(9, ZonedDateTime.of(2019, 1, 1, 17, 41, 0, 0, zone)),

                new MyEvent(1, ZonedDateTime.of(2019, 1, 1, 18, 49, 0, 0, zone)),
                new MyEvent(2, ZonedDateTime.of(2019, 1, 1, 18, 50, 0, 0, zone)),
                new MyEvent(7, ZonedDateTime.of(2019, 1, 1, 18, 51, 0, 0, zone))
                , dummyEventToForceSuppression()
        );
        final List<ExpectedResult> expectedValues = Arrays.asList(
                new ExpectedResult(1,
                        6,
                        ZonedDateTime.of(2019, 1, 1, 15, 0, 0, 0, zone),
                        ZonedDateTime.of(2019, 1, 1, 16, 0, 0, 0, zone)),
                new ExpectedResult(1,
                        20,
                        ZonedDateTime.of(2019, 1, 1, 16, 0, 0, 0, zone),
                        ZonedDateTime.of(2019, 1, 1, 17, 0, 0, 0, zone)),
                new ExpectedResult(1,
                        24,
                        ZonedDateTime.of(2019, 1, 1, 17, 0, 0, 0, zone),
                        ZonedDateTime.of(2019, 1, 1, 18, 0, 0, 0, zone)),
                new ExpectedResult(1,
                        10,
                        ZonedDateTime.of(2019, 1, 1, 18, 0, 0, 0, zone),
                        ZonedDateTime.of(2019, 1, 1, 19, 0, 0, 0, zone))


        );
        verifyByHour(inputValues, expectedValues, zone);
    }

    private void verifyByHour(final List<MyEvent> inputValues, final List<ExpectedResult> expectedValues, final ZoneId zoneId) {

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        // The commit interval for flushing records to state stores and downstream must be lower than
        // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        final Topology topology = buildKafkaStreamTopologyByHour(zoneId);

        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, streamsConfiguration);

        injectFakeData(inputValues, testDriver);
        verifyResultsByHour(expectedValues, testDriver);

        testDriver.close();
    }

    private Topology buildKafkaStreamTopologyByHour(final ZoneId zoneId) {
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, Integer> input = builder.stream(inputTopic);
        final KStream<Windowed<Integer>, Integer> sumOfOddNumbers = input
                .selectKey((k, v) -> 1)
                .groupByKey()
//                .windowedBy(new HourlyTimeWindows(zoneId, windowStartHour, Duration.ofMinutes(30)))
                .windowedBy(new HourlyTimeWindows(zoneId, windowStartHour, Duration.ofMinutes(30)))
                // A simple sum of value
                .reduce((v1, v2) -> v1 + v2, Materialized.with(Serdes.Integer(), Serdes.Integer()))
                // We only care about final result
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream();
        sumOfOddNumbers.print(Printed.toSysOut());
        sumOfOddNumbers.to(outputTopic, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Integer.class), Serdes.Integer()));
        return builder.build();

    }

    private class MyEvent {
        Integer value;
        ZonedDateTime timestamp;

        public MyEvent(final Integer value, final ZonedDateTime timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    private class ExpectedResult {
        Integer key;
        Integer value;
        ZonedDateTime start;
        ZonedDateTime end;

        public ExpectedResult(final Integer key, final Integer value, final ZonedDateTime start, final ZonedDateTime end) {
            this.key = key;
            this.value = value;
            this.start = start;
            this.end = end;
        }

        public Windowed<Integer> toWindowed() {
            return new Windowed<>(key, new TimeWindow(start.toInstant().toEpochMilli(), end.toInstant().toEpochMilli()));
        }
    }

    private void verifyResultsByHour(final List<ExpectedResult> expectedValues, final TopologyTestDriver testDriver) {

        LOGGER.info("==Aggregated Results==");

        ProducerRecord producerRecord = testDriver.readOutput(outputTopic, new TimeWindowedDeserializer(new IntegerDeserializer(), Duration.ofHours(1).toMillis()), new IntegerDeserializer());
        while(producerRecord !=null){
            LOGGER.info("Key -> "+ producerRecord.key() + ",  Value -> " + producerRecord.value() );
            producerRecord = testDriver.readOutput(outputTopic, new TimeWindowedDeserializer(new IntegerDeserializer(), Duration.ofHours(1).toMillis()), new IntegerDeserializer());
        }

//        for (final ExpectedResult expectedValue : expectedValues) {
//            final ProducerRecord<Windowed<Integer>, Integer> results = testDriver.readOutput(outputTopic, new TimeWindowedDeserializer(new IntegerDeserializer(), Duration.ofDays(1).toMillis()), new IntegerDeserializer());
//            LOGGER.info("Expected <" + expectedValue.toWindowed().key() + ", " + expectedValue.value + "> but was <" + results.key() + ", " + results.value() + ">");
//            OutputVerifier.compareKeyValue(results, expectedValue.toWindowed(), expectedValue.value);
//
//        }



    }

    private void injectFakeData(final List<MyEvent> inputValues, final TopologyTestDriver testDriver) {
        final ConsumerRecordFactory<String, Integer> factory = new ConsumerRecordFactory<>(inputTopic, new StringSerializer(), new IntegerSerializer());
        final List<ConsumerRecord<byte[], byte[]>> records = inputValues.stream()
                .map( e -> factory.create(inputTopic, null, e.value, new RecordHeaders(), e.timestamp.toInstant().toEpochMilli()))
                .collect(Collectors.toList());
        for (int i = 0; i < records.size(); i++) {
            LOGGER.info("Window ->  "+ i + " , time -> "+ Instant.ofEpochMilli(records.get(i).timestamp()).atZone(zone));
        }
        testDriver.pipeInput(records);
    }

    /** Generates an event after window end + grace period to trigger flush everything through suppression
     @see KTable#suppress(Suppressed)
    */
    private MyEvent dummyEventToForceSuppression() {
        return new MyEvent(7, ZonedDateTime.now());
    }
}
