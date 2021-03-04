package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class WindowedTumblingAggregateTopologyTest extends TopologyTestBase {

    private Instant start = Instant.parse("2019-04-20T10:35:00.00Z");

    @Override
    protected Supplier<Topology> withTopology() {
        // Window of 3 min width sliding by 3 min
        return new ParameterizedWindowedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC,
                TimeWindows.of(Duration.ofMinutes(3)).advanceBy(Duration.ofMinutes(3)), start);
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

        // first window starts here
        inputTopic.pipeInput("a", "0s", start.toEpochMilli());
        inputTopic.pipeInput("a", "60s", start.plus(60, ChronoUnit.SECONDS).toEpochMilli());
        inputTopic.pipeInput("a", "180s", start.plus(180, ChronoUnit.SECONDS).toEpochMilli());
        // second window starts here
        inputTopic.pipeInput("a", "240s", start.plus(240, ChronoUnit.SECONDS).toEpochMilli());
        // late arriving message for first window
        inputTopic.pipeInput("a", "120s (late)", start.plus(120, ChronoUnit.SECONDS).toEpochMilli());

        inputTopic.pipeInput("a", "360s", start.plus(360, ChronoUnit.SECONDS).toEpochMilli());
        inputTopic.pipeInput("a", "420s", start.plus(420, ChronoUnit.SECONDS).toEpochMilli());

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-120,60)", "0s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(60,240)", "60s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(60,240)", "60s,180s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(240,420)", "240s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(60,240)", "60s,180s,120s (late)"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(240,420)", "240s,360s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(420,600)", "420s"));

        assertThat(outputTopic.isEmpty()).isTrue();
    }
}
