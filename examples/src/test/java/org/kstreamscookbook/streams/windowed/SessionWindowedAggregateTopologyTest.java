package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class SessionWindowedAggregateTopologyTest extends TopologyTestBase {

    private Instant start = Instant.parse("2019-04-20T10:35:00.00Z");
    private Duration duration = Duration.ofSeconds(5);

    private TestInputTopic<String, String> inputTopic;

    @BeforeEach
    void localSetup() {
        // called after superclass @beforeEach
        var stringSerializer = new StringSerializer();

        inputTopic = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, stringSerializer);
    }

    @Override
    protected Supplier<Topology> withTopology() {
        return new SessionWindowedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC, duration);
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
    public void testSessionWindowedAggregation() {
        // first window starts here
        String key = "a";

        createRecord(key, 0);
        createRecord(key,1);
        createRecord(key, 2);
        createRecord(key, 3);

        // new session
        createRecord(key, 10);
        createRecord(key, 12);

        // late arrival, joins previous sessions
        createRecord(key, 7);
        createRecord(key, 8);

        // another session window

        createRecord(key, 20);
        createRecord(key, 21);

        // and another late arrival, this time merging the already merged window with the new window
        createRecord(key, 17);

        var stringDeserializer = new StringDeserializer();
        var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

        // the original window
        assertThat(outputTopic.readValue()).isEqualTo("0s");
        assertThat(outputTopic.readValue()).isEqualTo("0s,1s");
        assertThat(outputTopic.readValue()).isEqualTo("0s,1s,2s");
        assertThat(outputTopic.readValue()).isEqualTo("0s,1s,2s,3s");

        // a new session window
        assertThat(outputTopic.readValue()).isEqualTo("10s");
        assertThat(outputTopic.readValue()).isEqualTo("10s,12s");

        // we are expecting a merged window of "0s,1s,2s,3s" with "10s,12s" and 7s appended
        assertThat(outputTopic.readValue()).isEqualTo("[(0s,1s,2s,3s),(10s,12s)],7s");

        assertThat(outputTopic.readValue()).isEqualTo("[(0s,1s,2s,3s),(10s,12s)],7s,8s");

        // a new window again
        assertThat(outputTopic.readValue()).isEqualTo("20s");
        assertThat(outputTopic.readValue()).isEqualTo("20s,21s");

        // read this carefully - this is a merge of "[(0s,1s,2s,3s),(10s,12s)],7s,8s" and "20s,21s) with 17s appended
        assertThat(outputTopic.readValue()).isEqualTo("[([(0s,1s,2s,3s),(10s,12s)],7s,8s),(20s,21s)],17s");

        // No more records expected, window has expired

        assertThat(outputTopic.isEmpty()).isTrue();
    }

    /*
        Hide the details of the record creation here.
        Although we are trying to give you the raw calls most of the time we need to focus on the timing of the session
        window here.
     */
    private void createRecord(String key, long seconds) {
        String value = String.format("%ds",seconds);

        inputTopic.pipeInput(key, value, timestamp(seconds, ChronoUnit.SECONDS));
    }

    private long timestamp(long amount, TemporalUnit unit) {
        return start.plus(amount, unit).toEpochMilli();
    }
}