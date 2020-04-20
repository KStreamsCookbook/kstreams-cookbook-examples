package org.kstreamscookbook.streams.joins;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeWindowStreamInnerJoinStreamTopologyTest extends TopologyTestBase {
    private final static String LEFT_TOPIC = "left-topic";
    private final static String RIGHT_TOPIC = "right-topic";

    private Instant start = Instant.parse("2019-04-20T10:35:00.00Z");

    @Override
    protected Supplier<Topology> withTopology() {
        return new TimeWindowStreamInnerJoinStreamTopology(LEFT_TOPIC, RIGHT_TOPIC, OUTPUT_TOPIC);
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
    public void testStreamLeftJoinStream() {
        var stringSerializer = new StringSerializer();
        var leftTopic = testDriver.createInputTopic(LEFT_TOPIC, stringSerializer, stringSerializer);
        var rightTopic = testDriver.createInputTopic(RIGHT_TOPIC, stringSerializer, stringSerializer);

        leftTopic.pipeInput("a", "1<", timeOffset(0));

        rightTopic.pipeInput("a", ">1", timeOffset(0));
        rightTopic.pipeInput("a", ">2", timeOffset(1));

        rightTopic.pipeInput("b", ">3", timeOffset(3));
        leftTopic.pipeInput("b", "3<", timeOffset(4));

        leftTopic.pipeInput("b", "4<", timeOffset(5));

        // too late for previous window
        leftTopic.pipeInput("b", "5<", timeOffset(9));

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "1<>1"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", "1<>2"));

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "3<>3"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "4<>3"));

        // nothing to see here
        assertThat(outputTopic.isEmpty()).isTrue();

        // late arrival
        leftTopic.pipeInput("b", "5<", timeOffset(6));

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", "5<>3"));
    }

    /**
     * Returns a time point in milliseconds from epoch relative to the start point
     * @param seconds offset
     * @return time in milliseconds from epoch
     */
    private long timeOffset(long seconds) {
        return start.plusSeconds(seconds).toEpochMilli();
    }
}
