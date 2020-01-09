package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertNull;

class TimeWindowedAggregateTopologyTest extends TopologyTestBase {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();


    @Override
    protected Supplier<Topology> withTopology() {
        return new TimeWindowedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Override
    protected Map<String, String> withProperties() {
        // Use the timestamp of the message for windowing purposes.
        // The following classes are available to extract this timestamp, they vary on how they handle invalid timestamps:
        //
        // FailOnInvalidTimestamp - throws an exception
        // LogAndSkipOnInvalidTimestamp - logs a warning that the message will be discarded
        // UsePreviousTimeOnInvalidTimestamp - the latest extracted valid timestamp of the current record's partition
        return Collections.singletonMap(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, LogAndSkipOnInvalidTimestamp.class.getName());
    }

    @Test
    public void testWindowedAggregation() {
        var factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        var start = Instant.parse("2019-04-20T10:35:00.00Z");

        // first window starts here
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "1", start.toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "2", start.plus(1, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "3", start.plus(2, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "4", start.plus(3, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "bad message", -1L));
        // second window starts here
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "5", start.plus(5, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "6", start.plus(6, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "7", start.plus(7, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "8", start.plus(8, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "9", start.plus(9, ChronoUnit.MINUTES).toEpochMilli()));

        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "1 Day", start.plus(1, ChronoUnit.DAYS).toEpochMilli()));

        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "11", start.plus(8, ChronoUnit.MINUTES).toEpochMilli()));

        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "2 Days", start.plus(2, ChronoUnit.DAYS).toEpochMilli()));

        // beyond the implicit grace period of 1 day
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "12", start.plus(4, ChronoUnit.MINUTES).toEpochMilli()));


        OutputVerifier.compareValue(readNextRecord(),"1");
        OutputVerifier.compareValue(readNextRecord(), "1,2");
        OutputVerifier.compareValue(readNextRecord(), "1,2,3");
        OutputVerifier.compareValue(readNextRecord(), "1,2,3,4");
        OutputVerifier.compareValue(readNextRecord(), "5");
        OutputVerifier.compareValue(readNextRecord(), "5,6");
        OutputVerifier.compareValue(readNextRecord(), "5,6,7");
        OutputVerifier.compareValue(readNextRecord(), "5,6,7,8");
        OutputVerifier.compareValue(readNextRecord(), "5,6,7,8,9");

        OutputVerifier.compareValue(readNextRecord(), "1 Day");

        // Late arrival causes reissuing of record
        OutputVerifier.compareValue(readNextRecord(), "5,6,7,8,9,11");

        OutputVerifier.compareValue(readNextRecord(), "2 Days");

        // No more records expected, window has expired
        assertNull(readNextRecord());
    }

    private ProducerRecord<String, String> readNextRecord() {
        return testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
    }
}