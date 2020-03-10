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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

class TimeWindowedSuppressedAggregateTopologyTest extends TopologyTestBase {

    @Override
    protected Supplier<Topology> withTopology() {
        return new TimeWindowedSuppressedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC);
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

        // third window
        inputTopic.pipeInput("a", "10", start.plus(10, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "11", start.plus(11, ChronoUnit.MINUTES).toEpochMilli());

        // late arrival

        inputTopic.pipeInput("a", "9", start.plus(9, ChronoUnit.MINUTES).toEpochMilli());

        // fourth window
        inputTopic.pipeInput("a", "20", start.plus(20, ChronoUnit.MINUTES).toEpochMilli());
        inputTopic.pipeInput("a", "21", start.plus(21, ChronoUnit.MINUTES).toEpochMilli());

        inputTopic.pipeInput("a", "2 Days", start.plus(2, ChronoUnit.DAYS).toEpochMilli());

        // beyond the implicit grace period of 1 day
        inputTopic.pipeInput("a", "12", start.plus(12, ChronoUnit.MINUTES).toEpochMilli());

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readValue()).isEqualTo("1,2,3,4");
        assertThat(outputTopic.readValue()).isEqualTo("5,6,7,8,9");
        assertThat(outputTopic.readValue()).isEqualTo("10,11");
        assertThat(outputTopic.readValue()).isEqualTo("20,21");

        // 2 days is not completed yet, nothing to emit from the suppression
        assertThat(outputTopic.isEmpty()).isTrue();
    }

}