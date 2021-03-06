package org.kstreamscookbook.streams.windowed;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
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

public class WindowedSlidingAggregateTopologyTest extends TopologyTestBase {

  Instant start = Instant.parse("2019-04-20T10:35:00.00Z");

  @Override
  protected Supplier<Topology> withTopology() {
    // Sliding Window with 10 min time difference and 30 minute grace period
    Duration timeDifferenceMs = Duration.ofMinutes(3);
    Duration gracePeriodMs = Duration.ofMinutes(10);

    return new SlidingWindowedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC, start, timeDifferenceMs, gracePeriodMs);
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
    inputTopic.pipeInput("a", "0", start.toEpochMilli());
    inputTopic.pipeInput("a", "60s", start.plus(60, ChronoUnit.SECONDS).toEpochMilli());
    // second window starts here
    inputTopic.pipeInput("a", "90s", start.plus(90, ChronoUnit.SECONDS).toEpochMilli());
    // late arriving message for first window
    inputTopic.pipeInput("a", "45s (late)", start.plus(45, ChronoUnit.SECONDS).toEpochMilli());

    inputTopic.pipeInput("a", "120s", start.plus(120, ChronoUnit.SECONDS).toEpochMilli());

    var stringDeserializer = new StringDeserializer();
    var outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);

    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-180,0)", "0"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,180)", "60s"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-120,60)", "0,60s"));

    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,180)", "60s,90s"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(60,240)", "90s"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-90,90)", "0,60s,90s"));

    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,180)", "60s,90s,45s (late)"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-90,90)", "0,60s,90s,45s (late)"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-120,60)", "0,60s,45s (late)"));

    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-135,45)", "0,45s (late)"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(45,225)", "60s,90s"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(60,240)", "90s,120s"));

    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(45,225)", "60s,90s,120s"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(0,180)", "60s,90s,45s (late),120s"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(90,270)", "120s"));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a@(-60,120)", "0,60s,90s,45s (late),120s"));

    assertThat(outputTopic.isEmpty()).isTrue();
  }

}
