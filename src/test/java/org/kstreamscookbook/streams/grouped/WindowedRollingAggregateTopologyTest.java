package org.kstreamscookbook.streams.grouped;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.LogAndSkipOnInvalidTimestamp;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.jupiter.api.Test;
import org.kstreamscookbook.TopologyBuilder;
import org.kstreamscookbook.TopologyTestBase;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNull;

public class WindowedRollingAggregateTopologyTest extends TopologyTestBase {
  public static final String INPUT_TOPIC = "input-topic";
  public static final String OUTPUT_TOPIC = "output-topic";

  private StringSerializer stringSerializer = new StringSerializer();
  private StringDeserializer stringDeserializer = new StringDeserializer();

  Instant start = Instant.parse("2019-04-20T10:35:00.00Z");

  @Override
  protected TopologyBuilder withTopologyBuilder() {
    // Window of 3 min width sliding by 1 min
    return new WindowedAggregateTopology(INPUT_TOPIC, OUTPUT_TOPIC,
            TimeWindows.of(Duration.ofMinutes(3)).advanceBy(Duration.ofMinutes(1)),
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
    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    //
    // first window starts here
    testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "0", start.toEpochMilli()));
    testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "60s", start.plus(60, ChronoUnit.SECONDS).toEpochMilli()));
//    // second window starts here
    testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "90s", start.plus(90, ChronoUnit.SECONDS).toEpochMilli()));
//    // late arriving message for first window
    testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "45s (late)", start.plus(45, ChronoUnit.SECONDS).toEpochMilli()));

    testDriver.pipeInput(factory.create(INPUT_TOPIC, "a", "120s", start.plus(120, ChronoUnit.SECONDS).toEpochMilli()));


    OutputVerifier.compareKeyValue(readNextRecord(), "a@(-120,60)", "0");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(-60,120)", "0");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(0,180)", "0");

    OutputVerifier.compareKeyValue(readNextRecord(), "a@(-60,120)", "0,60s");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(0,180)", "0,60s");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(60,240)", "60s");

    OutputVerifier.compareKeyValue(readNextRecord(), "a@(-60,120)", "0,60s,90s");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(0,180)", "0,60s,90s");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(60,240)", "60s,90s");

    OutputVerifier.compareKeyValue(readNextRecord(), "a@(-120,60)", "0,45s (late)");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(-60,120)", "0,60s,90s,45s (late)");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(0,180)", "0,60s,90s,45s (late)");

    OutputVerifier.compareKeyValue(readNextRecord(), "a@(0,180)", "0,60s,90s,45s (late),120s");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(60,240)", "60s,90s,120s");
    OutputVerifier.compareKeyValue(readNextRecord(), "a@(120,300)", "120s");

    assertNull(readNextRecord());
  }

  private ProducerRecord<String, String> readNextRecord() {
    return testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
  }
}
