package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyBuilder;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

class WindowedGraceAggregateTopologyTest extends TopologyTestBase {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    private StringSerializer stringSerializer = new StringSerializer();
    private StringDeserializer stringDeserializer = new StringDeserializer();

    @Override
    protected TopologyBuilder withTopologyBuilder() {
        return new WindowedGraceAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC);
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
        ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

        //
        Instant start = Instant.parse("2019-04-20T10:35:00.00Z");
        // first window starts here
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "1", start.toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "2", start.plus(1, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "3", start.plus(2, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "4", start.plus(3, ChronoUnit.MINUTES).toEpochMilli()));
        // second window starts here
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "5", start.plus(5, ChronoUnit.MINUTES).toEpochMilli()));
        // late arriving message for first window
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "late", start.plus(4, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "6", start.plus(6, ChronoUnit.MINUTES).toEpochMilli()));
        testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "7", start.plus(8, ChronoUnit.MINUTES).toEpochMilli()));


        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1,2");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1,2,3");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1,2,3,4");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "5");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "1,2,3,4,late");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "5,6");
        OutputVerifier.compareKeyValue(readNextRecord(), "a", "5,6,7");
    }

    private ProducerRecord<String, String> readNextRecord() {
        return testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
    }
}