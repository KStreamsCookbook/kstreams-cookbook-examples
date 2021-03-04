package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class TimeWindowedCountTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new TimeWindowedCountTopology(INPUT_TOPIC, OUTPUT_TOPIC);
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
        var stringSerializer = new StringSerializer();
        var inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);

        var start = Instant.parse("2019-04-20T10:35:00.00Z");

        // first window starts here
        inputTopic.pipeInput("a", "1", start.toEpochMilli());
        inputTopic.pipeInput("a", "2", start.plus(1, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "3", start.plus(2, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "4", start.plus(3, ChronoUnit.MINUTES).toEpochMilli());

        // second window starts here
        inputTopic.pipeInput("a", "5", start.plus(5, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "6", start.plus(6, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "7", start.plus(7, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "8", start.plus(8, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "9", start.plus(9, ChronoUnit.MINUTES).toEpochMilli());

        inputTopic.pipeInput("a", "1 Day", start.plus(1, ChronoUnit.DAYS).toEpochMilli());

        inputTopic.pipeInput("a", "11", start.plus(8, ChronoUnit.MINUTES).toEpochMilli());

        inputTopic.pipeInput("a", "2 Days", start.plus(2, ChronoUnit.DAYS).toEpochMilli());

        // beyond the implicit grace period of 1 day
        inputTopic.pipeInput("a", "12", start.plus(4, ChronoUnit.MINUTES).toEpochMilli());

        var stringDeserializer = new StringDeserializer();
        var longDeserializer = new LongDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, longDeserializer);

        assertThat(outputTopic.readValue()).isEqualTo(1L);
        assertThat(outputTopic.readValue()).isEqualTo(2L);
        assertThat(outputTopic.readValue()).isEqualTo(3L);
        assertThat(outputTopic.readValue()).isEqualTo(4L);

        assertThat(outputTopic.readValue()).isEqualTo(1L);
        assertThat(outputTopic.readValue()).isEqualTo(2L);
        assertThat(outputTopic.readValue()).isEqualTo(3L);
        assertThat(outputTopic.readValue()).isEqualTo(4L);
        assertThat(outputTopic.readValue()).isEqualTo(5L);

        assertThat(outputTopic.readValue()).isEqualTo(1L);

        // Late arrival causes reissuing of record
        assertThat(outputTopic.readValue()).isEqualTo(6L);

        assertThat(outputTopic.readValue()).isEqualTo(1L);

        // No more records expected, window has expired
        assertThat(outputTopic.isEmpty()).isTrue();
    }

}