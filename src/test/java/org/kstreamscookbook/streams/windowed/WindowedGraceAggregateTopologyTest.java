package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

class WindowedGraceAggregateTopologyTest extends TopologyTestBase {

    private Instant start = Instant.parse("2019-04-20T10:35:00.00Z");

    @Override
    protected Supplier<Topology> withTopology() {
        return new ParameterizedWindowedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC,
                TimeWindows.of(Duration.ofSeconds(300)).grace(Duration.ofSeconds(120)),
                start);
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
        inputTopic.pipeInput("a", "120s", start.plus(120, ChronoUnit.SECONDS).toEpochMilli());
        inputTopic.pipeInput("a", "180s", start.plus(180, ChronoUnit.SECONDS).toEpochMilli());
        // second window starts here
        inputTopic.pipeInput("a", "300s", start.plus(300, ChronoUnit.SECONDS).toEpochMilli());
        // late arriving message for first window
        inputTopic.pipeInput("a", "240s (late)", start.plus(240, ChronoUnit.SECONDS).toEpochMilli());
        inputTopic.pipeInput("a", "360s", start.plus(360, ChronoUnit.SECONDS).toEpochMilli());
        inputTopic.pipeInput("a", "420s", start.plus(420, ChronoUnit.SECONDS).toEpochMilli());

        inputTopic.pipeInput("a", "240s (too late)", start.plus(4, ChronoUnit.SECONDS).toEpochMilli());

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,300)","0s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,300)","0s,60s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,300)","0s,60s,120s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,300)","0s,60s,120s,180s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(300,600)","300s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,300)","0s,60s,120s,180s,240s (late)"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(300,600)","300s,360s"));
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(300,600)","300s,360s,420s"));

        assertThat(outputTopic.isEmpty()).isTrue();
    }
}